use crate::catalog::{InformationSchemaCatalogs, InternalCatalog, CATALOGS_TABLE_NAME, DOBBYDB_SCHEMA_NAME, INTERNAL_CATALOG_NAME};
use crate::statements::ExtendedStatement;
use datafusion::catalog::streaming::StreamingTable;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::streaming::PartitionStream;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion::sql::TableReference;
use std::hash::Hash;
use std::sync::Arc;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::Session;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::{execute_stream, ExecutionPlan};

pub struct ExtendedQueryPlanner {
    session_context: SessionContext,
}

impl ExtendedQueryPlanner {
    pub fn new() -> Result<Self, DataFusionError> {
        let session_config = SessionConfig::new().with_information_schema(true);
        let session_context = SessionContext::new_with_config(session_config);
        let session_context = Self::register_something(session_context)?;
        if let Some(catalog) = session_context.catalog("INTERNAL") {
            if let Some(schema) = catalog.schema("DOBBYDB") {
                println!("succeed");
            }
        }
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
        session_context.register_catalog(INTERNAL_CATALOG_NAME, Arc::new(InternalCatalog::new()));

        let catalog_table = TableReference::full(INTERNAL_CATALOG_NAME, DOBBYDB_SCHEMA_NAME, CATALOGS_TABLE_NAME);
        session_context.register_table(
            catalog_table,
            Arc::new(ExtendedQueryPlanner::wrap_with_stream_table(Arc::new(
                InformationSchemaCatalogs::new()?,
            ))?),
        )?;
        Ok(session_context)
    }

    pub async fn create_logical_plan(
        &self,
        statement: ExtendedStatement,
    ) -> Result<LogicalPlan, DataFusionError> {
        match statement {
            ExtendedStatement::SQLStatement(stmt) => {
                let sql_string = stmt.to_string();
                let df = self.session_context.sql(&sql_string).await?;
                Ok(df.logical_plan().clone())
            }
            ExtendedStatement::ShowCatalogsStatement => {
                let sql_string = "SELECT * FROM internal.dobbydb.catalogs";
                let df = self.session_context.sql(&sql_string).await?;
                Ok(df.logical_plan().clone())
            }
        }
    }

    pub async fn create_physical_plan(&self, logical_plan: &LogicalPlan) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        self.session_context.state().create_physical_plan(&logical_plan).await
    }

    pub async fn execute_physical_plan(&self, physical_plan: Arc<dyn ExecutionPlan>) -> Result<SendableRecordBatchStream, DataFusionError> {
        let task_ctx = self.session_context.task_ctx();
        let batch = execute_stream(physical_plan, task_ctx);
        batch
    }
}
