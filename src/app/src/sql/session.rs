use crate::catalog::{
    CatalogManager, DobbyDbCatalogProviderList, INFORMATION_SCHEMA_SHOW_CATALOGS,
    INFORMATION_SCHEMA_SHOW_SCHEMAS, INFORMATION_SCHEMA_SHOW_TABLES,
    INFORMATION_SCHEMA_SHOW_VARIABLES, INTERNAL_CATALOG,
};
use crate::context::DobbyDbContext;
use crate::parser::ExtendedParser;
use crate::statements::{ExtendedStatement, ShowCatalogsStatement};
use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::AsyncCatalogProviderList;
use datafusion::catalog::information_schema::INFORMATION_SCHEMA;
use datafusion::common::Result;
use datafusion::config::ConfigOptions;
use datafusion::dataframe::DataFrame;
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::logical_expr::sqlparser::ast::{
    ObjectName, ObjectNamePart, ShowCreateObject, ShowStatementFilter, ShowStatementFilterPosition,
    ShowStatementOptions, Statement, Use,
};
use datafusion::logical_expr::{ExplainFormat, LogicalPlanBuilder};
use datafusion::prelude::{SessionConfig, SessionContext};
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
                self.build_show_catalogs_sql(show_catalogs)?
            }
            ExtendedStatement::ShowVariablesStatement(show_variables) => {
                self.build_show_variables_sql(&show_variables.filter, show_variables.verbose)?
            }
            ExtendedStatement::SQLStatement(stmt) => match stmt.as_ref() {
                Statement::Use(use_stmt) => {
                    return self.handle_use_stmt(use_stmt).await;
                }
                Statement::ShowSchemas { show_options, .. } => {
                    self.build_show_schemas_sql(show_options)?
                }
                Statement::ShowTables { show_options, .. } => {
                    self.build_show_tables_sql(show_options)?
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
        let references = state.resolve_table_references(&statement)?;
        // Now we can asynchronously resolve the table references to get a cached catalog
        // that we can use for our query
        let catalog_provider_list = DobbyDbCatalogProviderList::new(self.dobbydb_context.clone());
        let resolved_catalog_providers = catalog_provider_list
            .resolve(&references, state.config())
            .await?;
        self.session_context
            .register_catalog_list(resolved_catalog_providers);
        match &statement {
            datafusion::sql::parser::Statement::Statement(stmt) => match stmt.as_ref() {
                Statement::ShowCreate { obj_type, obj_name } => {
                    return self.handle_show_create_stmt(obj_type, obj_name).await;
                }
                _ => {
                    // do nothing
                }
            },
            _ => {
                // do nothing
            }
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

    fn build_show_catalogs_sql(&self, show_catalogs: &ShowCatalogsStatement) -> Result<String> {
        self.build_filtered_virtual_table_sql(
            INFORMATION_SCHEMA_SHOW_CATALOGS,
            "catalog_name",
            &show_catalogs.filter,
        )
    }

    fn build_show_schemas_sql(&self, show_options: &ShowStatementOptions) -> Result<String> {
        self.build_filtered_virtual_table_sql(
            INFORMATION_SCHEMA_SHOW_SCHEMAS,
            "schema_name",
            &Self::show_like_filter(show_options),
        )
    }

    fn build_show_tables_sql(&self, show_options: &ShowStatementOptions) -> Result<String> {
        self.build_filtered_virtual_table_sql(
            INFORMATION_SCHEMA_SHOW_TABLES,
            "table_name",
            &Self::show_like_filter(show_options),
        )
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

    fn build_filtered_virtual_table_sql(
        &self,
        table_name: &str,
        column_name: &str,
        filter: &Option<ShowStatementFilter>,
    ) -> Result<String> {
        let base_sql = format!(
            "SELECT * FROM {}.{}.{}",
            INTERNAL_CATALOG, INFORMATION_SCHEMA, table_name
        );

        let base_sql = match filter {
            None => base_sql,
            Some(ShowStatementFilter::Like(pattern)) => {
                format!(
                    "{base_sql} WHERE {column_name} LIKE '{}'",
                    Self::escape_sql_string(pattern)
                )
            }
            _ => base_sql,
        };

        Ok(format!("{base_sql} ORDER BY {column_name}"))
    }

    fn escape_sql_string(value: &str) -> String {
        value.replace('\'', "''")
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
    async fn test_show_tables_like_execution() -> Result<()> {
        let session = ExtendedSessionContext::default();
        let batches = session
            .sql("show tables like '%table%'")
            .await?
            .collect()
            .await?;
        let output = pretty_format_batches(&batches)?.to_string();
        assert_contains!(output.as_str(), "tables");
        Ok(())
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
    fn test_show_like_sql_escapes_quotes() -> Result<()> {
        let session = ExtendedSessionContext::default();
        let sql = session.build_filtered_virtual_table_sql(
            INFORMATION_SCHEMA_SHOW_CATALOGS,
            "catalog_name",
            &Some(ShowStatementFilter::Like("%o''hara%".to_string())),
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
