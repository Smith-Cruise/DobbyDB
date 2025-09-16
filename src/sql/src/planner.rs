use crate::statements::ExtendedStatement;
use datafusion::catalog::Session;
use datafusion::catalog::information_schema::INFORMATION_SCHEMA;
use datafusion::catalog::streaming::StreamingTable;
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::streaming::PartitionStream;
use datafusion::physical_plan::{ExecutionPlan, execute_stream};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion::sql::TableReference;
use dobbydb_catalog::internal_catalog::{
    INFORMATION_SCHEMA_CATALOGS, INFORMATION_SCHEMA_SCHEMAS, INFORMATION_SCHEMA_TABLES,
    INTERNAL_CATALOG, InformationSchemaShowCatalogs, InternalCatalog,
};
use sqlparser::ast::Statement;
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
    session_context: SessionContext,
}

impl ExtendedQueryPlanner {
    pub fn new() -> Result<Self, DataFusionError> {
        let session_config = SessionConfig::new();
        let session_context = SessionContext::new_with_config(session_config);
        let session_context = Self::register_something(session_context)?;
        Ok(Self { session_context })
    }

    fn wrap_with_stream_table(
        table: Arc<dyn PartitionStream>,
    ) -> Result<StreamingTable, DataFusionError> {
        Ok(StreamingTable::try_new(
            Arc::clone(&table.schema()),
            vec![table],
        )?)
    }

    fn register_something(
        session_context: SessionContext,
    ) -> Result<SessionContext, DataFusionError> {
        session_context.register_catalog(INTERNAL_CATALOG, Arc::new(InternalCatalog::new()));

        let catalog_table = TableReference::full(
            INTERNAL_CATALOG,
            INFORMATION_SCHEMA,
            INFORMATION_SCHEMA_TABLES,
        );
        session_context.register_table(
            catalog_table,
            Arc::new(ExtendedQueryPlanner::wrap_with_stream_table(Arc::new(
                InformationSchemaShowCatalogs::new()?,
            ))?),
        )?;
        Ok(session_context)
    }

    pub async fn create_logical_plan(
        &self,
        statement: &ExtendedStatement,
    ) -> Result<LogicalPlan, DataFusionError> {
        let mut sql_string: Option<String> = None;
        match statement {
            ExtendedStatement::ShowCatalogsStatement => {
                sql_string = Some(
                    ScanVirtualTableSqlBuilder::new_with_table_reference(TableReference::Bare {
                        table: Arc::from(INFORMATION_SCHEMA_CATALOGS),
                    })
                    .build_sql(),
                );
            }
            ExtendedStatement::SQLStatement(stmt) => match stmt.as_ref() {
                Statement::ShowSchemas { .. } => {
                    sql_string = Some(
                        ScanVirtualTableSqlBuilder::new_with_table_reference(
                            TableReference::Bare {
                                table: Arc::from(INFORMATION_SCHEMA_SCHEMAS),
                            },
                        )
                        .build_sql(),
                    );
                }
                Statement::ShowTables { .. } => {
                    sql_string = Some(String::from(
                        ScanVirtualTableSqlBuilder::new_with_table_reference(
                            TableReference::Bare {
                                table: Arc::from(INFORMATION_SCHEMA_TABLES),
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
}
