use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::parquet::ParquetFileReader;
use datafusion::datasource::physical_plan::{ParquetFileMetrics, ParquetFileReaderFactory};
use datafusion::object_store::ObjectStore;
use datafusion::parquet::arrow::async_reader::{AsyncFileReader, ParquetObjectReader};
use datafusion::physical_expr_common::metrics::ExecutionPlanMetricsSet;
use std::sync::Arc;
use tokio::runtime::Handle;

#[derive(Debug)]
pub struct ExtendedParquetFileReaderFactory {
    io_handle: Handle,
    store: Arc<dyn ObjectStore>,
}

impl ExtendedParquetFileReaderFactory {
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        let io_handle = Handle::current();
        Self { store, io_handle }
    }
}

impl ParquetFileReaderFactory for ExtendedParquetFileReaderFactory {
    fn create_reader(
        &self,
        partition_index: usize,
        partitioned_file: PartitionedFile,
        metadata_size_hint: Option<usize>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> datafusion::common::Result<Box<dyn AsyncFileReader + Send>> {
        let file_metrics = ParquetFileMetrics::new(
            partition_index,
            partitioned_file.object_meta.location.as_ref(),
            metrics,
        );
        let store = Arc::clone(&self.store);
        let mut inner =
            ParquetObjectReader::new(store, partitioned_file.object_meta.location.clone())
                .with_file_size(partitioned_file.object_meta.size)
                .with_runtime(self.io_handle.clone());

        if let Some(hint) = metadata_size_hint {
            inner = inner.with_footer_size_hint(hint)
        };

        Ok(Box::new(ParquetFileReader {
            inner,
            file_metrics,
            partitioned_file,
        }))
    }
}
