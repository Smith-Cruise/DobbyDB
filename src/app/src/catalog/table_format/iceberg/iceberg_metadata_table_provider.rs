use crate::table_format::iceberg::iceberg_metadata_scan::IcebergMetadataTableScan;
use crate::table_format::metadata_table::MetadataTableType;
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::DataFusionError;
use datafusion::common::Result;
use datafusion::datasource::TableType;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use futures::TryStreamExt;
use futures::stream::BoxStream;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::inspect::MetadataTableType as IcebergMetadataTableType;
use iceberg::table::Table;
use std::any::Any;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub(crate) struct IcebergMetadataTableProvider {
    table: Table,
    r#type: IcebergMetadataTableType,
}

impl IcebergMetadataTableProvider {
    pub fn try_new(
        table: Table,
        metadata_table_type: MetadataTableType,
    ) -> Result<IcebergMetadataTableProvider> {
        let metadata_table_type = match metadata_table_type {
            MetadataTableType::Snapshots => IcebergMetadataTableType::Snapshots,
            MetadataTableType::Manifests => IcebergMetadataTableType::Manifests,
            MetadataTableType::DataFiles | MetadataTableType::Partitions => {
                return Err(DataFusionError::NotImplemented(format!(
                    "iceberg metadata table {:?} is not supported",
                    metadata_table_type
                )));
            }
        };
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
            IcebergMetadataTableType::Snapshots => metadata_table.snapshots().schema(),
            IcebergMetadataTableType::Manifests => metadata_table.manifests().schema(),
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
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(IcebergMetadataTableScan::new(self.clone())))
    }
}

impl IcebergMetadataTableProvider {
    pub async fn scan(self) -> Result<BoxStream<'static, Result<RecordBatch>>> {
        let metadata_table = self.table.inspect();
        let stream = match self.r#type {
            IcebergMetadataTableType::Snapshots => metadata_table.snapshots().scan().await,
            IcebergMetadataTableType::Manifests => metadata_table.manifests().scan().await,
        }
        .map_err(|e| DataFusionError::External(e.into()))?;
        let stream = stream.map_err(|e| DataFusionError::External(e.into()));
        Ok(Box::pin(stream))
    }
}
