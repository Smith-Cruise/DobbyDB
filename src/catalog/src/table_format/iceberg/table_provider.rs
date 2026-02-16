use crate::storage::StorageCredential;
use crate::table_format::iceberg::table_scan::IcebergTableScanBuilder;
use async_trait::async_trait;
use datafusion::arrow::datatypes::Schema;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::Result;
use datafusion::common::{DataFusionError, ToDFSchema};
use datafusion::datasource::TableType;
use datafusion::logical_expr::utils::conjunction;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::table::Table;
use std::any::Any;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct IcebergTableProvider {
    table: Table,
    snapshot_id: Option<i64>,
    schema: Arc<Schema>,
    storage_credential: Option<StorageCredential>,
}

impl IcebergTableProvider {
    pub async fn try_new_from_table(
        table: Table,
        storage_credential: Option<StorageCredential>,
    ) -> Result<Self> {
        let schema = Arc::new(
            schema_to_arrow_schema(table.metadata().current_schema())
                .map_err(|e| DataFusionError::External(Box::new(e)))?,
        );
        Ok(IcebergTableProvider {
            table,
            snapshot_id: None,
            schema,
            storage_credential,
        })
    }
}

#[async_trait]
impl TableProvider for IcebergTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut builder = IcebergTableScanBuilder::new(self.table.clone(), self.schema.clone());

        let df_schema = self.schema.as_ref().clone().to_dfschema()?;
        let predicate = conjunction(filters.iter().cloned())
            .and_then(|predicate| state.create_physical_expr(predicate, &df_schema).ok());

        let metadata_location = if let Some(metadata_location) = self.table.metadata_location() {
            metadata_location
        } else {
            Err(DataFusionError::Configuration(
                "metadata location not found.".into(),
            ))?
        };
        if let Some(credential) = &self.storage_credential {
            credential.register_into_session(metadata_location, state)?;
        }
        builder = builder
            .with_snapshot_id(self.snapshot_id)
            .with_projection(projection)
            .with_filters(Some(filters))
            .with_predicate(predicate);
        Ok(Arc::new(builder.build().await?))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        // Push down all filters, as a single source of truth, the scanner will drop the filters which couldn't be push down
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }
}
