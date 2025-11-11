use crate::table_format::iceberg::metadata_scan::IcebergMetadataScan;
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::DataFusionError;
use datafusion::datasource::TableType;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use futures::TryStreamExt;
use futures::stream::BoxStream;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::inspect::MetadataTableType;
use iceberg::table::Table;
use std::any::Any;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub(crate) struct IcebergMetadataTableProvider {
    table: Table,
    r#type: MetadataTableType,
}

impl IcebergMetadataTableProvider {
    pub fn try_new(
        table: Table,
        metadata_table_name: &str,
    ) -> Result<IcebergMetadataTableProvider, DataFusionError> {
        let metadata_table_type = MetadataTableType::try_from(metadata_table_name)
            .map_err(|e| DataFusionError::NotImplemented(e))?;
        Ok(IcebergMetadataTableProvider {
            table,
            r#type: metadata_table_type,
        })
    }
}

#[async_trait]
impl TableProvider for IcebergMetadataTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        let metadata_table = self.table.inspect();
        let schema = match self.r#type {
            MetadataTableType::Snapshots => metadata_table.snapshots().schema(),
            MetadataTableType::Manifests => metadata_table.manifests().schema(),
        };
        schema_to_arrow_schema(&schema).unwrap().into()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(IcebergMetadataScan::new(self.clone())))
    }
}

impl IcebergMetadataTableProvider {
    pub async fn scan(
        self,
    ) -> Result<BoxStream<'static, Result<RecordBatch, DataFusionError>>, DataFusionError> {
        let metadata_table = self.table.inspect();
        let stream = match self.r#type {
            MetadataTableType::Snapshots => metadata_table.snapshots().scan().await,
            MetadataTableType::Manifests => metadata_table.manifests().scan().await,
        }
        .map_err(|e| DataFusionError::External(e.into()))?;
        let stream = stream.map_err(|e| DataFusionError::External(e.into()));
        Ok(Box::pin(stream))
    }
}
