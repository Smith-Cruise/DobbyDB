mod show;
mod r#use;

use crate::catalog::{CatalogManager, DobbyDbCatalogProviderList, INTERNAL_CATALOG};
use crate::context::DobbyDbContext;
use crate::parser::ExtendedParser;
use crate::statements::ExtendedStatement;
use datafusion::catalog::AsyncCatalogProviderList;
use datafusion::catalog::information_schema::INFORMATION_SCHEMA;
use datafusion::common::Result;
use datafusion::config::ConfigOptions;
use datafusion::dataframe::DataFrame;
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::logical_expr::ExplainFormat;
use datafusion::logical_expr::sqlparser::ast::Statement;
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
    use url::Url;

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
