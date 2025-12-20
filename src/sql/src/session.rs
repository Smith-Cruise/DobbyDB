use crate::parser::ExtendedParser;
use crate::statements::ExtendedStatement;
use datafusion::catalog::information_schema::INFORMATION_SCHEMA;
use datafusion::catalog::{CatalogProviderList, MemoryCatalogProviderList};
use datafusion::common::Result;
use datafusion::common::TableReference;
use datafusion::dataframe::DataFrame;
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::sqlparser::ast::{Statement, Use};
use datafusion::logical_expr::LogicalPlanBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use dobbydb_catalog::catalog::get_catalog_manager;
use dobbydb_catalog::internal_catalog::{
    InternalCatalog, INFORMATION_SCHEMA_SHOW_CATALOGS,
    INFORMATION_SCHEMA_SHOW_SCHEMAS, INFORMATION_SCHEMA_SHOW_TABLES, INTERNAL_CATALOG,
};
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

struct ScanVirtualTableSqlBuilder {
    catalog_name: String,
    schema_name: String,
    table_name: String,
}

impl ScanVirtualTableSqlBuilder {
    fn new_with_table_reference(table_reference: TableReference) -> ScanVirtualTableSqlBuilder {
        match table_reference {
            TableReference::Bare { table } => Self {
                catalog_name: String::from(INTERNAL_CATALOG),
                schema_name: String::from(INFORMATION_SCHEMA),
                table_name: String::from(table.as_ref()),
            },
            TableReference::Partial { schema, table } => Self {
                catalog_name: String::from(INTERNAL_CATALOG),
                schema_name: String::from(schema.as_ref()),
                table_name: String::from(table.as_ref()),
            },
            TableReference::Full {
                catalog,
                schema,
                table,
            } => Self {
                catalog_name: String::from(catalog.as_ref()),
                schema_name: String::from(schema.as_ref()),
                table_name: String::from(table.as_ref()),
            },
        }
    }

    fn build_sql(&self) -> String {
        format!(
            "SELECT * FROM {}.{}.{}",
            self.catalog_name, self.schema_name, self.table_name
        )
    }
}

pub struct ExtendedSessionContext {
    session_context: SessionContext,
}

impl ExtendedSessionContext {
    pub async fn new() -> Result<Self> {
        let session_config = SessionConfig::new()
            .with_default_catalog_and_schema(INTERNAL_CATALOG, INFORMATION_SCHEMA);
        let session_context = SessionContext::new_with_config(session_config);
        let memory_catalog_provider_list = Arc::new(MemoryCatalogProviderList::new());

        let catalog_manager = get_catalog_manager().read().unwrap();
        catalog_manager
            .register_into_catalog_provider_list(memory_catalog_provider_list.clone())
            .await?;

        // load internal catalog
        memory_catalog_provider_list.register_catalog(
            INTERNAL_CATALOG.to_string(),
            Arc::new(InternalCatalog::try_new(memory_catalog_provider_list.clone()).await?),
        );

        session_context.register_catalog_list(memory_catalog_provider_list);
        Ok(Self { session_context })
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
        let sql_string: Option<String>;
        match statement {
            ExtendedStatement::ShowCatalogsStatement => {
                sql_string = Some(
                    ScanVirtualTableSqlBuilder::new_with_table_reference(TableReference::Bare {
                        table: Arc::from(INFORMATION_SCHEMA_SHOW_CATALOGS),
                    })
                    .build_sql(),
                );
            }
            ExtendedStatement::SQLStatement(stmt) => match stmt.as_ref() {
                Statement::Use(use_stmt) => {
                    return self.handle_use_stmt(&use_stmt).await;
                }
                Statement::ShowSchemas { .. } => {
                    sql_string = Some(
                        ScanVirtualTableSqlBuilder::new_with_table_reference(
                            TableReference::Bare {
                                table: Arc::from(INFORMATION_SCHEMA_SHOW_SCHEMAS),
                            },
                        )
                        .build_sql(),
                    );
                }
                Statement::ShowTables { .. } => {
                    sql_string = Some(String::from(
                        ScanVirtualTableSqlBuilder::new_with_table_reference(
                            TableReference::Bare {
                                table: Arc::from(INFORMATION_SCHEMA_SHOW_TABLES),
                            },
                        )
                        .build_sql(),
                    ));
                }
                _ => {
                    sql_string = Some(stmt.to_string());
                }
            },
        }

        if let Some(sql) = sql_string {
            self.session_context.sql(&sql).await
        } else {
            Err(DataFusionError::Plan(String::from(
                "failed to create logical plan",
            )))
        }
    }

    pub fn task_ctx(&self) -> Arc<TaskContext> {
        self.session_context.task_ctx()
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
                        return Err(DataFusionError::Plan(String::from(format!(
                            "unsupported use stmt {:?}",
                            use_stmt
                        ))));
                    }
                }
            }
            Use::Catalog(catalog_name) => {
                let object_name_vec = &catalog_name.0;
                (object_name_vec[0].to_string(), None)
            }
            _ => {
                return Err(DataFusionError::Plan(String::from(format!(
                    "unsupported use stmt {:?}",
                    use_stmt
                ))));
            }
        };

        let catalog_provider = match self.session_context.catalog(&catalog_name) {
            Some(catalog_provider) => catalog_provider,
            None => {
                return Err(DataFusionError::Plan(String::from(format!(
                    "unknown catalog {}",
                    catalog_name
                ))));
            }
        };

        let state = self.session_context.state_ref();
        state
            .write()
            .config_mut()
            .options_mut()
            .catalog
            .default_catalog = catalog_name.clone();

        if let Some(schema_name) = schema_name {
            match catalog_provider.schema(&schema_name) {
                None => {
                    return Err(DataFusionError::Plan(String::from(format!(
                        "unknown schema {}",
                        schema_name
                    ))));
                }
                _ => {}
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
