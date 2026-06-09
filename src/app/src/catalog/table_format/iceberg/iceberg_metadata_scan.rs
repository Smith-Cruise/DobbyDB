use crate::table_format::iceberg::iceberg_metadata_table_provider::IcebergMetadataTableProvider;
use datafusion::catalog::TableProvider;
use datafusion::common::Result;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, ExecutionPlan, Partitioning, PlanProperties};
use futures::TryStreamExt;
use std::sync::Arc;

#[derive(Debug)]
pub struct IcebergMetadataTableScan {
    provider: IcebergMetadataTableProvider,
    properties: Arc<PlanProperties>,
}

impl IcebergMetadataTableScan {
    pub fn new(provider: IcebergMetadataTableProvider) -> Self {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(provider.schema()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            provider,
            properties,
        }
    }
}

impl DisplayAs for IcebergMetadataTableScan {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "IcebergMetadataTableScan")
    }
}

impl ExecutionPlan for IcebergMetadataTableScan {
    fn name(&self) -> &str {
        "IcebergMetadataTableScan"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&std::sync::Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: std::sync::Arc<Self>,
        _children: Vec<std::sync::Arc<dyn ExecutionPlan>>,
    ) -> Result<std::sync::Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: std::sync::Arc<datafusion::execution::TaskContext>,
    ) -> Result<datafusion::execution::SendableRecordBatchStream> {
        let fut = self.provider.clone().scan();
        let stream = futures::stream::once(fut).try_flatten();
        let schema = self.provider.schema();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}
