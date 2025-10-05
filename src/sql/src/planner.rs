use crate::statements::ExtendedStatement;
use datafusion::catalog::information_schema::INFORMATION_SCHEMA;
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder};
use datafusion::physical_plan::{execute_stream, ExecutionPlan};
use datafusion::prelude::SessionContext;
use datafusion::sql::TableReference;
use dobbydb_catalog::internal_catalog::{
    INFORMATION_SCHEMA_SHOW_CATALOGS, INFORMATION_SCHEMA_SHOW_SCHEMAS,
    INFORMATION_SCHEMA_SHOW_TABLES, INTERNAL_CATALOG,
};
use sqlparser::ast::{Statement, Use};
use std::sync::Arc;

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

pub struct ExtendedQueryPlanner {
    session_context: Arc<SessionContext>,
}

impl ExtendedQueryPlanner {
    pub fn new(session_context: Arc<SessionContext>) -> Result<Self, DataFusionError> {
        Ok(Self { session_context })
    }

    pub async fn create_logical_plan(
        &self,
        statement: &ExtendedStatement,
    ) -> Result<LogicalPlan, DataFusionError> {
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
            let df = self.session_context.sql(&sql).await?;
            Ok(df.logical_plan().clone())
        } else {
            Err(DataFusionError::Plan(String::from(
                "failed to create logical plan",
            )))
        }
    }

    pub async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        self.session_context
            .state()
            .create_physical_plan(&logical_plan)
            .await
    }

    pub async fn execute_physical_plan(
        &self,
        physical_plan: Arc<dyn ExecutionPlan>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        let task_ctx = self.session_context.task_ctx();
        let batch = execute_stream(physical_plan, task_ctx);
        batch
    }

    async fn handle_use_stmt(&self, use_stmt: &Use) -> Result<LogicalPlan, DataFusionError> {
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

        Ok(LogicalPlanBuilder::empty(false).build()?)
    }
}
