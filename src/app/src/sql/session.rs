use crate::catalog::{
    CatalogConfig, CatalogManager, DobbyDbCatalogProviderList, INFORMATION_SCHEMA_SHOW_VARIABLES,
    INTERNAL_CATALOG,
};
use crate::context::DobbyDbContext;
use crate::parser::ExtendedParser;
use crate::statements::{ExtendedStatement, ShowCatalogsStatement};
use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::AsyncCatalogProviderList;
use datafusion::catalog::information_schema::INFORMATION_SCHEMA;
use datafusion::common::{Result, TableReference};
use datafusion::config::ConfigOptions;
use datafusion::dataframe::DataFrame;
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::logical_expr::sqlparser::ast::{
    ObjectName, ObjectNamePart, ShowCreateObject, ShowStatementFilter, ShowStatementFilterPosition,
    ShowStatementInParentType, ShowStatementOptions, Statement, Use,
};
use datafusion::logical_expr::{ExplainFormat, LogicalPlanBuilder};
use datafusion::prelude::{SessionConfig, SessionContext, col, lit};
use datafusion::sql::parser::Statement as DataFusionStatement;
use dobbydb_common::runtime::RuntimeManager;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock, RwLock};

static SESSION_MANAGER: OnceLock<RwLock<HashMap<i64, Arc<SessionContext>>>> = OnceLock::new();

fn get_session_manager() -> &'static RwLock<HashMap<i64, Arc<SessionContext>>> {
    SESSION_MANAGER.get_or_init(|| RwLock::new(HashMap::new()))
}

pub struct SessionManager {}

impl SessionManager {
    pub fn register_session(
        session_id: i64,
        session_context: Arc<SessionContext>,
    ) -> Result<Arc<SessionContext>> {
        let manager = get_session_manager();
        let mut map = manager
            .write()
            .map_err(|e| DataFusionError::Internal(e.to_string()))?;
        if let Some(value) = map.insert(session_id, session_context) {
            Ok(value.clone())
        } else {
            Err(DataFusionError::Internal("failed to insert session".into()))
        }
    }

    pub fn get_session(session_id: i64) -> Result<Arc<SessionContext>> {
        let manager = get_session_manager();
        let map = manager
            .read()
            .map_err(|e| DataFusionError::Internal(e.to_string()))?;
        if let Some(value) = map.get(&session_id) {
            Ok(value.clone())
        } else {
            Err(DataFusionError::Internal(format!(
                "session {} is not found",
                session_id
            )))
        }
    }
}

pub struct ExtendedSessionContext {
    dobbydb_context: Arc<DobbyDbContext>,
    session_context: SessionContext,
}

impl Default for ExtendedSessionContext {
    fn default() -> Self {
        let dobbydb_context = Arc::new(DobbyDbContext {
            server_config: Default::default(),
            catalog_manager: Arc::new(CatalogManager::default()),
            runtime_manager: Arc::new(RuntimeManager::default()),
            default_catalog: None,
            default_schema: None,
        });
        let runtime_env = Arc::new(RuntimeEnv::default());
        Self::new(dobbydb_context, runtime_env)
    }
}

impl ExtendedSessionContext {
    pub fn new(dobbydb_context: Arc<DobbyDbContext>, runtime_env: Arc<RuntimeEnv>) -> Self {
        let catalog = dobbydb_context
            .default_catalog
            .as_deref()
            .unwrap_or(INTERNAL_CATALOG);
        let schema = dobbydb_context
            .default_schema
            .as_deref()
            .unwrap_or(INFORMATION_SCHEMA);
        let mut options = ConfigOptions::new();
        options.explain.format = ExplainFormat::Tree;
        let session_config =
            SessionConfig::from(options).with_default_catalog_and_schema(catalog, schema);
        let session_context = SessionContext::new_with_config_rt(session_config, runtime_env);
        Self {
            session_context,
            dobbydb_context,
        }
    }

    pub async fn sql(&self, sql: &str) -> Result<DataFrame> {
        let parser = ExtendedParser::parse_sql(sql)?;
        if parser.len() != 1 {
            return Err(DataFusionError::Execution(format!(
                "Invalid query: {}",
                sql
            )));
        }

        let stmt = &parser[0];
        self.create_dataframe(stmt).await
    }

    pub async fn create_dataframe(&self, statement: &ExtendedStatement) -> Result<DataFrame> {
        let sql_string: String = match statement {
            ExtendedStatement::ShowCatalogsStatement(show_catalogs) => {
                return self.handle_show_catalogs(show_catalogs);
            }
            ExtendedStatement::ShowVariablesStatement(show_variables) => {
                self.build_show_variables_sql(&show_variables.filter, show_variables.verbose)?
            }
            ExtendedStatement::SQLStatement(stmt) => match stmt.as_ref() {
                Statement::Use(use_stmt) => {
                    return self.handle_use_stmt(use_stmt).await;
                }
                Statement::ShowSchemas { show_options, .. } => {
                    return self.handle_show_schemas(show_options).await;
                }
                Statement::ShowTables { show_options, .. } => {
                    return self.handle_show_tables(show_options).await;
                }
                _ => stmt.to_string(),
            },
        };

        // Instead, to use a remote catalog, we must use lower level APIs on
        // SessionState (what `SessionContext::sql` does internally).
        let state = self.session_context.state();
        // First, parse the SQL (but don't plan it / resolve any table references)
        let dialect = state.config().options().sql_parser.dialect;
        let statement = state.sql_to_statement(&sql_string, &dialect)?;
        // Find all `TableReferences` in the parsed queries. These correspond to the
        // tables referred to by the query (in this case
        // `remote_schema.remote_table`)

        // DataFusion resolves SHOW CREATE through information_schema internally,
        // which adds synthetic references such as information_schema.columns.
        // Remote catalogs should only resolve the target table here; otherwise
        // HMS/Glue would try to load those synthetic information_schema tables.
        let references = match Self::show_create_statement(&statement) {
            Some((obj_type, obj_name)) => {
                self.resolve_show_create_table_references(obj_type, obj_name)?
            }
            None => state.resolve_table_references(&statement)?,
        };

        // Now we can asynchronously resolve the table references to get a cached catalog
        // that we can use for our query
        let catalog_provider_list = DobbyDbCatalogProviderList::new(self.dobbydb_context.clone());
        let resolved_catalog_providers = catalog_provider_list
            .resolve(&references, state.config())
            .await?;
        self.session_context
            .register_catalog_list(resolved_catalog_providers);
        if let Some((obj_type, obj_name)) = Self::show_create_statement(&statement) {
            return self.handle_show_create_stmt(obj_type, obj_name).await;
        }
        self.session_context.sql(&sql_string).await
    }

    pub fn task_ctx(&self) -> Arc<TaskContext> {
        self.session_context.task_ctx()
    }

    pub fn session_context(&self) -> &SessionContext {
        &self.session_context
    }

    fn build_show_variables_sql(
        &self,
        filter: &Option<ShowStatementFilter>,
        verbose: bool,
    ) -> Result<String> {
        let columns = if verbose {
            "name, value, description"
        } else {
            "name, value"
        };
        let base_sql = format!(
            "SELECT {columns} FROM {}.{}.{}",
            INTERNAL_CATALOG, INFORMATION_SCHEMA, INFORMATION_SCHEMA_SHOW_VARIABLES
        );

        let base_sql = match filter {
            None => base_sql,
            Some(ShowStatementFilter::Like(pattern)) => {
                format!(
                    "{base_sql} WHERE name LIKE '{}'",
                    Self::escape_sql_string(pattern)
                )
            }
            _ => {
                return Err(DataFusionError::Plan(
                    "SHOW VARIABLES only supports LIKE filters".to_string(),
                ));
            }
        };
        Ok(format!("{base_sql} ORDER BY NAME"))
    }

    fn show_like_filter(show_options: &ShowStatementOptions) -> Option<ShowStatementFilter> {
        match &show_options.filter_position {
            None => None,
            Some(ShowStatementFilterPosition::Infix(filter))
            | Some(ShowStatementFilterPosition::Suffix(filter)) => match filter {
                ShowStatementFilter::Like(pattern) => {
                    Some(ShowStatementFilter::Like(pattern.clone()))
                }
                _ => None,
            },
        }
    }

    fn handle_show_catalogs(&self, show_catalogs: &ShowCatalogsStatement) -> Result<DataFrame> {
        let catalogs = self.dobbydb_context.catalog_manager.list_catalogs();
        let mut catalog_names = Vec::with_capacity(catalogs.len());
        let mut catalog_types = Vec::with_capacity(catalogs.len());
        let mut catalog_configs = Vec::with_capacity(catalogs.len());

        for (catalog_name, catalog_config) in catalogs {
            catalog_names.push(catalog_name);
            match catalog_config {
                CatalogConfig::Internal => {
                    catalog_types.push("INTERNAL".to_string());
                    catalog_configs.push(None);
                }
                CatalogConfig::HMS(config) => {
                    catalog_types.push("HMS".to_string());
                    catalog_configs.push(Some(format!("{config:?}")));
                }
                CatalogConfig::GLUE(config) => {
                    catalog_types.push("GLUE".to_string());
                    catalog_configs.push(Some(format!("{config:?}")));
                }
            }
        }

        let schema = Arc::new(Schema::new(vec![
            Field::new("catalog_name", DataType::Utf8, false),
            Field::new("catalog_type", DataType::Utf8, false),
            Field::new("catalog_config", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(catalog_names)),
                Arc::new(StringArray::from(catalog_types)),
                Arc::new(StringArray::from(catalog_configs)),
            ],
        )
        .map_err(|error| DataFusionError::External(Box::new(error)))?;
        let mut dataframe = self.session_context.read_batch(batch)?;
        if let Some(ShowStatementFilter::Like(pattern)) = &show_catalogs.filter {
            dataframe = dataframe.filter(col("catalog_name").like(lit(pattern.clone())))?;
        }
        dataframe.sort(vec![col("catalog_name").sort(true, false)])
    }

    async fn handle_show_schemas(&self, show_options: &ShowStatementOptions) -> Result<DataFrame> {
        let default_catalog = self
            .session_context
            .state()
            .config()
            .options()
            .catalog
            .default_catalog
            .clone();
        let catalog_name = Self::resolve_show_scope(show_options, "SHOW SCHEMAS", 1)?
            .map_or_else(|| default_catalog, |parts| parts[0].clone());

        let schema_names = self
            .dobbydb_context
            .catalog_manager
            .list_schema_names(&catalog_name)
            .await?;
        self.build_show_names_dataframe(
            "schema_name",
            schema_names,
            Self::show_like_filter(show_options),
        )
    }

    async fn handle_show_tables(&self, show_options: &ShowStatementOptions) -> Result<DataFrame> {
        let state = self.session_context.state();
        let catalog_options = &state.config().options().catalog;
        let default_catalog = catalog_options.default_catalog.clone();
        let default_schema = catalog_options.default_schema.clone();
        let (catalog_name, schema_name) =
            match Self::resolve_show_scope(show_options, "SHOW TABLES", 2)? {
                None => (default_catalog, default_schema),
                Some(parts) if parts.len() == 1 => (default_catalog, parts[0].clone()),
                Some(parts) => (parts[0].clone(), parts[1].clone()),
            };

        let table_names = self
            .dobbydb_context
            .catalog_manager
            .list_table_names(&catalog_name, &schema_name)
            .await?;
        self.build_show_names_dataframe(
            "table_name",
            table_names,
            Self::show_like_filter(show_options),
        )
    }

    fn resolve_show_scope(
        show_options: &ShowStatementOptions,
        statement_name: &str,
        max_parts: usize,
    ) -> Result<Option<Vec<String>>> {
        let Some(show_in) = &show_options.show_in else {
            return Ok(None);
        };
        if show_in.parent_type.as_ref().is_some_and(|parent_type| {
            !matches!(
                parent_type,
                ShowStatementInParentType::Database | ShowStatementInParentType::Schema
            )
        }) {
            return Err(DataFusionError::Plan(format!(
                "{statement_name} does not support {} scope",
                show_in.parent_type.as_ref().unwrap()
            )));
        }
        let parent_name = show_in.parent_name.as_ref().ok_or_else(|| {
            DataFusionError::Plan(format!("{statement_name} requires a scope name"))
        })?;
        let parts = parent_name
            .0
            .iter()
            .map(Self::convert_object_name_part_to_string)
            .collect::<Result<Vec<_>>>()?;
        if parts.is_empty() || parts.len() > max_parts {
            return Err(DataFusionError::Plan(format!(
                "{statement_name} supports at most {max_parts} scope name part(s)"
            )));
        }
        Ok(Some(parts))
    }

    fn convert_object_name_part_to_string(part: &ObjectNamePart) -> Result<String> {
        part.as_ident()
            .map(|ident| ident.value.clone())
            .ok_or_else(|| {
                DataFusionError::Plan(format!("unsupported SHOW scope name part: {part}"))
            })
    }

    fn build_show_names_dataframe(
        &self,
        column_name: &str,
        names: Vec<String>,
        filter: Option<ShowStatementFilter>,
    ) -> Result<DataFrame> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            column_name,
            DataType::Utf8,
            false,
        )]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(names))])
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        let mut dataframe = self.session_context.read_batch(batch)?;
        if let Some(ShowStatementFilter::Like(pattern)) = filter {
            dataframe = dataframe.filter(col(column_name).like(lit(pattern)))?;
        }
        dataframe.sort(vec![col(column_name).sort(true, false)])
    }

    fn escape_sql_string(value: &str) -> String {
        value.replace('\'', "''")
    }

    fn show_create_statement(
        statement: &DataFusionStatement,
    ) -> Option<(&ShowCreateObject, &ObjectName)> {
        match statement {
            DataFusionStatement::Statement(stmt) => match stmt.as_ref() {
                Statement::ShowCreate { obj_type, obj_name } => Some((obj_type, obj_name)),
                _ => None,
            },
            _ => None,
        }
    }

    fn resolve_show_create_table_references(
        &self,
        obj_type: &ShowCreateObject,
        obj_name: &ObjectName,
    ) -> Result<Vec<TableReference>> {
        if obj_type != &ShowCreateObject::Table {
            return Err(DataFusionError::NotImplemented(format!(
                "SHOW CREATE {obj_type} is not implemented"
            )));
        }

        let (catalog_name, schema_name, table_name) =
            self.resolve_show_create_table_name(obj_name)?;
        if table_name.contains('$') {
            return Err(DataFusionError::NotImplemented(
                "SHOW CREATE TABLE is not implemented for metadata tables".to_string(),
            ));
        }

        Ok(vec![TableReference::full(
            catalog_name,
            schema_name,
            table_name,
        )])
    }

    async fn handle_show_create_stmt(
        &self,
        obj_type: &ShowCreateObject,
        obj_name: &ObjectName,
    ) -> Result<DataFrame> {
        if obj_type != &ShowCreateObject::Table {
            return Err(DataFusionError::NotImplemented(format!(
                "SHOW CREATE {obj_type} is not implemented"
            )));
        }

        let (catalog_name, schema_name, table_name) =
            self.resolve_show_create_table_name(obj_name)?;
        if table_name.contains('$') {
            return Err(DataFusionError::NotImplemented(
                "SHOW CREATE TABLE is not implemented for metadata tables".to_string(),
            ));
        }

        let catalog_provider = self
            .session_context
            .catalog(&catalog_name)
            .ok_or_else(|| DataFusionError::Plan(format!("unknown catalog {}", catalog_name)))?;
        let schema_provider = catalog_provider
            .schema(&schema_name)
            .ok_or_else(|| DataFusionError::Plan(format!("unknown schema {}", schema_name)))?;
        let table_provider = schema_provider.table(&table_name).await?.ok_or_else(|| {
            DataFusionError::Plan(format!(
                "table '{}.{}.{}' not found",
                catalog_name, schema_name, table_name
            ))
        })?;
        let definition = table_provider.get_table_definition().ok_or_else(|| {
            DataFusionError::NotImplemented(format!(
                "SHOW CREATE TABLE is not implemented for table {catalog_name}.{schema_name}.{table_name}"
            ))
        })?;

        self.build_show_create_table_dataframe(definition.to_string())
    }

    fn build_show_create_table_dataframe(&self, definition: String) -> Result<DataFrame> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "definition",
            DataType::Utf8,
            false,
        )]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec![definition]))])
                .map_err(|error| DataFusionError::External(Box::new(error)))?;

        self.session_context.read_batch(batch)
    }

    fn resolve_show_create_table_name(
        &self,
        obj_name: &ObjectName,
    ) -> Result<(String, String, String)> {
        let parts = obj_name
            .0
            .iter()
            .map(Self::object_name_part_value)
            .collect::<Result<Vec<_>>>()?;
        let state = self.session_context.state();
        let catalog = &state.config().options().catalog;

        match parts.as_slice() {
            [table_name] => Ok((
                catalog.default_catalog.clone(),
                catalog.default_schema.clone(),
                table_name.clone(),
            )),
            [schema_name, table_name] => Ok((
                catalog.default_catalog.clone(),
                schema_name.clone(),
                table_name.clone(),
            )),
            [catalog_name, schema_name, table_name] => Ok((
                catalog_name.clone(),
                schema_name.clone(),
                table_name.clone(),
            )),
            _ => Err(DataFusionError::Plan(format!(
                "unsupported SHOW CREATE TABLE name: {obj_name}"
            ))),
        }
    }

    fn object_name_part_value(part: &ObjectNamePart) -> Result<String> {
        part.as_ident()
            .map(|ident| ident.value.clone())
            .ok_or_else(|| {
                DataFusionError::Plan(format!("unsupported SHOW CREATE TABLE name part: {part}"))
            })
    }

    async fn handle_use_stmt(&self, use_stmt: &Use) -> Result<DataFrame> {
        let (catalog_name, schema_name) = match use_stmt {
            Use::Object(object_name) => {
                let object_name_vec = &object_name.0;
                match object_name_vec.len() {
                    1 => (
                        self.session_context
                            .state()
                            .config()
                            .options()
                            .catalog
                            .default_catalog
                            .clone(),
                        Some(object_name_vec[0].to_string()),
                    ),
                    2 => (
                        object_name_vec[0].to_string(),
                        Some(object_name_vec[1].to_string()),
                    ),
                    _ => {
                        return Err(DataFusionError::Plan(format!(
                            "unsupported use stmt {:?}",
                            use_stmt
                        )));
                    }
                }
            }
            Use::Catalog(catalog_name) => {
                let object_name_vec = &catalog_name.0;
                (object_name_vec[0].to_string(), None)
            }
            _ => {
                return Err(DataFusionError::Plan(format!(
                    "unsupported use stmt {:?}",
                    use_stmt
                )));
            }
        };

        if !self
            .dobbydb_context
            .catalog_manager
            .catalog_exists(&catalog_name)
        {
            return Err(DataFusionError::Plan(format!(
                "unknown catalog {}",
                catalog_name
            )));
        }

        let state = self.session_context.state_ref();
        state
            .write()
            .config_mut()
            .options_mut()
            .catalog
            .default_catalog = catalog_name.clone();

        if let Some(schema_name) = schema_name {
            if !self
                .dobbydb_context
                .catalog_manager
                .schema_exist(&catalog_name, &schema_name)
                .await?
            {
                return Err(DataFusionError::Plan(format!(
                    "unknown schema {}",
                    schema_name
                )));
            }

            state
                .write()
                .config_mut()
                .options_mut()
                .catalog
                .default_schema = schema_name.clone();
        }

        let empty_logical_plan = LogicalPlanBuilder::empty(false).build()?;
        self.session_context
            .execute_logical_plan(empty_logical_plan)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::common::assert_contains;
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    use datafusion::object_store::memory::InMemory;
    use datafusion::object_store::path::Path;
    use datafusion::object_store::{ObjectStoreExt, PutPayload};
    use datafusion::prelude::CsvReadOptions;
    use datafusion_cli::object_storage::instrumented::{
        InstrumentedObjectStoreMode, InstrumentedObjectStoreRegistry,
    };
    use std::sync::Arc;
    use url::Url;

    #[tokio::test]
    async fn test_show_variables_execution() -> Result<()> {
        let session = ExtendedSessionContext::default();
        let batches = session.sql("show variables").await?.collect().await?;
        let schema = batches[0].schema();
        let output = pretty_format_batches(&batches)?.to_string();

        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "name");
        assert_eq!(schema.field(1).name(), "value");
        assert_contains!(output.as_str(), "datafusion.execution.target_partitions");
        Ok(())
    }

    #[tokio::test]
    async fn test_show_variables_verbose_execution() -> Result<()> {
        let session = ExtendedSessionContext::default();
        let batches = session
            .sql("show variables verbose")
            .await?
            .collect()
            .await?;
        let schema = batches[0].schema();
        let output = pretty_format_batches(&batches)?.to_string();

        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.field(0).name(), "name");
        assert_eq!(schema.field(1).name(), "value");
        assert_eq!(schema.field(2).name(), "description");
        assert_contains!(output.as_str(), "datafusion.execution.target_partitions");
        Ok(())
    }

    #[tokio::test]
    async fn test_show_variables_like_execution() -> Result<()> {
        let session = ExtendedSessionContext::default();
        let batches = session
            .sql("show variables like '%target_partitions%'")
            .await?
            .collect()
            .await?;
        let output = pretty_format_batches(&batches)?.to_string();

        assert_contains!(output, "datafusion.execution.target_partitions");
        Ok(())
    }

    #[tokio::test]
    async fn test_show_variables_verbose_like_execution() -> Result<()> {
        let session = ExtendedSessionContext::default();
        let batches = session
            .sql("show variables verbose like '%target_partitions%'")
            .await?
            .collect()
            .await?;
        let schema = batches[0].schema();
        let output = pretty_format_batches(&batches)?.to_string();

        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.field(2).name(), "description");
        assert_contains!(output.as_str(), "datafusion.execution.target_partitions");
        Ok(())
    }

    #[tokio::test]
    async fn test_show_catalogs_execution() -> Result<()> {
        let session = ExtendedSessionContext::default();
        let batches = session.sql("show catalogs").await?.collect().await?;
        let schema = batches[0].schema();
        let output = pretty_format_batches(&batches)?.to_string();

        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.field(0).name(), "catalog_name");
        assert_contains!(output.as_str(), "internal");
        Ok(())
    }

    #[tokio::test]
    async fn test_show_catalogs_like_execution() -> Result<()> {
        let session = ExtendedSessionContext::default();
        let batches = session
            .sql("show catalogs like '%tern%'")
            .await?
            .collect()
            .await?;
        let output = pretty_format_batches(&batches)?.to_string();

        assert_contains!(output.as_str(), "internal");
        Ok(())
    }

    #[tokio::test]
    async fn test_show_schemas_like_execution() -> Result<()> {
        let session = ExtendedSessionContext::default();
        let batches = session
            .sql("show schemas like '%formation%'")
            .await?
            .collect()
            .await?;
        let output = pretty_format_batches(&batches)?.to_string();

        assert_contains!(output.as_str(), "information_schema");
        Ok(())
    }

    #[tokio::test]
    async fn test_show_schemas_from_catalog_like_execution() -> Result<()> {
        let session = ExtendedSessionContext::default();
        let batches = session
            .sql("show schemas from internal like 'information_schem_'")
            .await?
            .collect()
            .await?;
        let output = pretty_format_batches(&batches)?.to_string();

        assert_contains!(output.as_str(), "information_schema");
        Ok(())
    }

    #[tokio::test]
    async fn test_show_tables_like_execution() -> Result<()> {
        let session = ExtendedSessionContext::default();
        let batches = session
            .sql("show tables like '%variable%'")
            .await?
            .collect()
            .await?;
        let output = pretty_format_batches(&batches)?.to_string();
        assert_contains!(output.as_str(), "variables");
        Ok(())
    }

    #[tokio::test]
    async fn test_show_tables_from_schema_execution() -> Result<()> {
        let session = ExtendedSessionContext::default();
        let batches = session
            .sql("show tables from information_schema")
            .await?
            .collect()
            .await?;
        let output = pretty_format_batches(&batches)?.to_string();

        assert_contains!(output.as_str(), "variables");
        Ok(())
    }

    #[tokio::test]
    async fn test_show_tables_from_catalog_schema_like_execution() -> Result<()> {
        let session = ExtendedSessionContext::default();
        let batches = session
            .sql("show tables from internal.information_schema like '%variable%'")
            .await?
            .collect()
            .await?;
        let output = pretty_format_batches(&batches)?.to_string();

        assert_contains!(output.as_str(), "variables");
        Ok(())
    }

    #[tokio::test]
    async fn test_show_tables_from_internal_information_schema_no_catalog_virtual_tables()
    -> Result<()> {
        let session = ExtendedSessionContext::default();
        let batches = session
            .sql("show tables from internal.information_schema")
            .await?
            .collect()
            .await?;
        let output = pretty_format_batches(&batches)?.to_string();

        assert_contains!(output.as_str(), "variables");
        assert!(!output.contains("catalogs"));
        assert!(!output.contains("schemas"));
        assert!(!output.contains("tables"));
        Ok(())
    }

    #[tokio::test]
    async fn test_internal_information_schema_variables_execution() -> Result<()> {
        let session = ExtendedSessionContext::default();
        let batches = session
            .sql("select name, value from internal.information_schema.variables limit 1")
            .await?
            .collect()
            .await?;

        assert_eq!(batches[0].schema().field(0).name(), "name");
        assert_eq!(batches[0].schema().field(1).name(), "value");
        Ok(())
    }

    #[tokio::test]
    async fn test_show_scope_errors() {
        let session = ExtendedSessionContext::default();

        let error = session
            .sql("show schemas from missing_catalog")
            .await
            .unwrap_err();
        assert_contains!(error.to_string(), "unknown catalog missing_catalog");

        let error = session
            .sql("show tables from missing_schema")
            .await
            .unwrap_err();
        assert_contains!(error.to_string(), "schema missing_schema not exist");

        let error = session
            .sql("show tables from internal.information_schema.extra")
            .await
            .unwrap_err();
        assert_contains!(error.to_string(), "supports at most 2 scope name part(s)");
    }

    #[test]
    fn test_resolve_show_create_table_name() -> Result<()> {
        let session = ExtendedSessionContext::default();

        let stmt = ExtendedParser::parse_sql("show create table tbl")?;
        if let ExtendedStatement::SQLStatement(stmt) = &stmt[0]
            && let Statement::ShowCreate { obj_name, .. } = stmt.as_ref()
        {
            assert_eq!(
                session.resolve_show_create_table_name(obj_name)?,
                (
                    INTERNAL_CATALOG.to_string(),
                    INFORMATION_SCHEMA.to_string(),
                    "tbl".to_string()
                )
            );
        } else {
            panic!("expected SHOW CREATE TABLE statement");
        }

        let stmt = ExtendedParser::parse_sql("show create table db.tbl")?;
        if let ExtendedStatement::SQLStatement(stmt) = &stmt[0]
            && let Statement::ShowCreate { obj_name, .. } = stmt.as_ref()
        {
            assert_eq!(
                session.resolve_show_create_table_name(obj_name)?,
                (
                    INTERNAL_CATALOG.to_string(),
                    "db".to_string(),
                    "tbl".to_string()
                )
            );
        } else {
            panic!("expected SHOW CREATE TABLE statement");
        }

        let stmt = ExtendedParser::parse_sql("show create table catalog.db.tbl")?;
        if let ExtendedStatement::SQLStatement(stmt) = &stmt[0]
            && let Statement::ShowCreate { obj_name, .. } = stmt.as_ref()
        {
            assert_eq!(
                session.resolve_show_create_table_name(obj_name)?,
                ("catalog".to_string(), "db".to_string(), "tbl".to_string())
            );
        } else {
            panic!("expected SHOW CREATE TABLE statement");
        }

        Ok(())
    }

    #[test]
    fn test_resolve_show_create_table_references_only_target_table() -> Result<()> {
        let session = ExtendedSessionContext::default();
        let stmt = ExtendedParser::parse_sql("show create table tbl")?;
        if let ExtendedStatement::SQLStatement(stmt) = &stmt[0]
            && let Statement::ShowCreate { obj_name, obj_type } = stmt.as_ref()
        {
            assert_eq!(
                session.resolve_show_create_table_references(obj_type, obj_name)?,
                vec![TableReference::full(
                    INTERNAL_CATALOG,
                    INFORMATION_SCHEMA,
                    "tbl"
                )]
            );
        } else {
            panic!("expected SHOW CREATE TABLE statement");
        }

        Ok(())
    }

    #[test]
    fn test_resolve_show_create_table_references_rejects_metadata_table() -> Result<()> {
        let session = ExtendedSessionContext::default();
        let stmt = ExtendedParser::parse_sql("show create table \"tbl$data_files\"")?;
        if let ExtendedStatement::SQLStatement(stmt) = &stmt[0]
            && let Statement::ShowCreate { obj_name, obj_type } = stmt.as_ref()
        {
            let err = session
                .resolve_show_create_table_references(obj_type, obj_name)
                .unwrap_err();
            assert_contains!(err.to_string(), "metadata tables");
        } else {
            panic!("expected SHOW CREATE TABLE statement");
        }

        Ok(())
    }

    #[test]
    fn test_show_like_sql_escapes_quotes() -> Result<()> {
        let session = ExtendedSessionContext::default();
        let sql = session.build_show_variables_sql(
            &Some(ShowStatementFilter::Like("%o''hara%".to_string())),
            false,
        )?;

        assert_contains!(sql.as_str(), "LIKE '%o''''hara%'");
        Ok(())
    }

    #[tokio::test]
    async fn test_instrumented_object_store_registry_records_requests() -> Result<()> {
        let instrumented_registry = Arc::new(
            InstrumentedObjectStoreRegistry::new()
                .with_profile_mode(InstrumentedObjectStoreMode::Summary),
        );
        let runtime_env = RuntimeEnvBuilder::new()
            .with_object_store_registry(instrumented_registry.clone())
            .build_arc()?;
        let dobbydb_context = Arc::new(DobbyDbContext::default());
        let session = ExtendedSessionContext::new(dobbydb_context, runtime_env);

        let store_url = Url::parse("memory://bucket").unwrap();
        let object_store = Arc::new(InMemory::new());
        object_store
            .put(
                &Path::from("data.csv"),
                PutPayload::from_static(b"id,value\n1,alpha\n2,beta\n"),
            )
            .await
            .map_err(|err| DataFusionError::External(Box::new(err)))?;
        session
            .session_context()
            .register_object_store(&store_url, object_store);
        session
            .session_context()
            .register_csv(
                "instrumented_csv",
                "memory://bucket/data.csv",
                CsvReadOptions::default(),
            )
            .await?;

        let batches = session
            .session_context()
            .sql("select count(*) as cnt from instrumented_csv")
            .await?
            .collect()
            .await?;
        let output = pretty_format_batches(&batches)?.to_string();

        assert_contains!(output.as_str(), "| 2   |");
        assert!(!instrumented_registry.stores().is_empty());

        let requests = instrumented_registry.stores()[0].take_requests();
        assert!(!requests.is_empty());
        Ok(())
    }
}
