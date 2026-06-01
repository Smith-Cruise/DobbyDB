use crate::table_format::hive::hive_partition::HivePartition;
use crate::table_format::hive::hive_storage_info::HiveStorageInfo;
use crate::table_format::hive::hive_table_provider::list_files;
use crate::table_format::metadata_table::MetadataTableType;
use async_trait::async_trait;
use datafusion::arrow::array::{StringArray, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::memory::DataSourceExec;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{DataFusionError, Result};
use datafusion::datasource::TableType;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::object_store::{ObjectMeta, ObjectStore};
use datafusion::physical_plan::ExecutionPlan;
use dobbydb_storage::storage::{Storage, parse_location_schema_bucket};
use futures::StreamExt;
use std::any::Any;
use std::future::Future;
use std::sync::Arc;
use url::Url;

#[derive(Debug)]
pub struct HiveMetadataTableProvider {
    hive_storage_info: HiveStorageInfo,
    partitions: Vec<HivePartition>,
    metadata_table_type: MetadataTableType,
    storage: Option<Storage>,
    schema: SchemaRef,
}

impl HiveMetadataTableProvider {
    pub fn try_new(
        hive_storage_info: HiveStorageInfo,
        partitions: Vec<HivePartition>,
        metadata_table_type: MetadataTableType,
        storage: Option<Storage>,
    ) -> Result<Self> {
        let schema = match metadata_table_type {
            MetadataTableType::FilePath => file_path_schema(),
            _ => {
                return Err(DataFusionError::NotImplemented(format!(
                    "hive metadata table {:?} is not supported",
                    metadata_table_type
                )));
            }
        };

        Ok(Self {
            hive_storage_info,
            partitions,
            metadata_table_type,
            storage,
            schema,
        })
    }

    async fn scan_file_path(&self, state: &dyn Session) -> Result<Vec<HiveFileMetadata>> {
        if let Some(storage) = &self.storage {
            storage.try_register_into_session(&self.hive_storage_info.table_location, state)?;
        }

        let (path_schema, path_bucket) =
            parse_location_schema_bucket(&self.hive_storage_info.table_location)?;
        let store_url = ObjectStoreUrl::parse(format!("{}://{}", path_schema, path_bucket))?;
        let object_store = state.runtime_env().object_store(&store_url)?;

        if self.partitions.is_empty() {
            return list_hive_file_metadata(
                state,
                &object_store,
                &self.hive_storage_info.table_location,
            )
            .await;
        }

        let meta_fetch_concurrency = state.config_options().execution.meta_fetch_concurrency;
        let partition_locations = self
            .partitions
            .iter()
            .map(|partition| partition.location.clone())
            .collect::<Vec<_>>();
        let partition_scan_tasks = partition_locations.into_iter().map(|location| {
            let object_store = Arc::clone(&object_store);

            async move { list_hive_file_metadata(state, &object_store, &location).await }
        });

        collect_metadata_files(partition_scan_tasks, meta_fetch_concurrency).await
    }

    fn build_file_path_batch(&self, files: Vec<HiveFileMetadata>) -> Result<RecordBatch> {
        let file_paths = StringArray::from(
            files
                .iter()
                .map(|file| file.path.as_str())
                .collect::<Vec<_>>(),
        );
        let file_sizes = UInt64Array::from(files.iter().map(|file| file.size).collect::<Vec<_>>());

        RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![Arc::new(file_paths), Arc::new(file_sizes)],
        )
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
    }
}

#[async_trait]
impl TableProvider for HiveMetadataTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let batch = match self.metadata_table_type {
            MetadataTableType::FilePath => {
                let mut files = self.scan_file_path(state).await?;
                if let Some(limit) = limit {
                    files.truncate(limit);
                }
                self.build_file_path_batch(files)?
            }
            _ => {
                return Err(DataFusionError::NotImplemented(format!(
                    "hive metadata table {:?} is not supported",
                    self.metadata_table_type
                )));
            }
        };
        let source = MemorySourceConfig::try_new(
            &[vec![batch]],
            Arc::clone(&self.schema),
            projection.cloned(),
        )?;
        Ok(DataSourceExec::from_data_source(source.with_limit(limit)))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }
}

fn file_path_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("file_path", DataType::Utf8, false),
        Field::new("file_size", DataType::UInt64, false),
    ]))
}

#[derive(Debug, PartialEq, Eq)]
struct HiveFileMetadata {
    path: String,
    size: u64,
}

async fn list_hive_file_metadata(
    state: &dyn Session,
    object_store: &Arc<dyn ObjectStore>,
    directory_full_location: &str,
) -> Result<Vec<HiveFileMetadata>> {
    let files = list_files(state, object_store, directory_full_location).await?;
    files
        .into_iter()
        .map(|file| object_meta_to_hive_file_metadata(directory_full_location, file))
        .collect()
}

fn object_meta_to_hive_file_metadata(
    directory_full_location: &str,
    file: ObjectMeta,
) -> Result<HiveFileMetadata> {
    Ok(HiveFileMetadata {
        path: object_meta_full_location(directory_full_location, &file)?,
        size: file.size,
    })
}

fn object_meta_full_location(directory_full_location: &str, file: &ObjectMeta) -> Result<String> {
    let parsed =
        Url::parse(directory_full_location).map_err(|e| DataFusionError::External(e.into()))?;
    let bucket_location = match parsed.host_str() {
        Some(host) => format!("{}://{}", parsed.scheme(), host),
        None => format!("{}://", parsed.scheme()),
    };

    Ok(format!(
        "{}/{}",
        bucket_location.trim_end_matches('/'),
        file.location.as_ref().trim_start_matches('/')
    ))
}

async fn collect_metadata_files<I, Fut>(
    tasks: I,
    concurrency: usize,
) -> Result<Vec<HiveFileMetadata>>
where
    I: IntoIterator<Item = Fut>,
    Fut: Future<Output = Result<Vec<HiveFileMetadata>>>,
{
    let files = futures::stream::iter(tasks)
        .buffer_unordered(concurrency)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?;

    Ok(files.into_iter().flatten().collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Array;
    use datafusion::arrow::datatypes::Schema;
    use datafusion::datasource::table_schema::TableSchema;
    use datafusion::object_store::ObjectStoreExt;
    use datafusion::object_store::memory::InMemory;
    use datafusion::object_store::path::Path;
    use datafusion::physical_plan::collect;
    use datafusion::prelude::SessionContext;
    use std::collections::HashMap;

    #[tokio::test]
    async fn test_scan_file_path_metadata_for_unpartitioned_table() {
        let ctx = session_context_with_store();
        let store = test_store(&ctx);
        put_test_object(&store, "hive/table/file1.parquet", b"123").await;
        put_test_object(&store, "hive/table/file2.parquet", b"12345").await;

        let provider = test_provider("s3://warehouse/hive/table", vec![]);
        let files = provider.scan_file_path(&ctx.state()).await.unwrap();

        assert_eq!(
            sorted_files(files),
            vec![
                ("s3://warehouse/hive/table/file1.parquet".to_string(), 3),
                ("s3://warehouse/hive/table/file2.parquet".to_string(), 5),
            ]
        );
    }

    #[tokio::test]
    async fn test_scan_file_path_metadata_for_partitioned_table() {
        let ctx = session_context_with_store();
        let store = test_store(&ctx);
        put_test_object(&store, "hive/table/dt=2024-01-01/file1.parquet", b"1").await;
        put_test_object(&store, "hive/table/dt=2024-01-02/file2.parquet", b"22").await;

        let provider = test_provider(
            "s3://warehouse/hive/table",
            vec![
                HivePartition {
                    location: "s3://warehouse/hive/table/dt=2024-01-01".to_string(),
                    partition_values: vec!["2024-01-01".to_string()],
                },
                HivePartition {
                    location: "s3://warehouse/hive/table/dt=2024-01-02".to_string(),
                    partition_values: vec!["2024-01-02".to_string()],
                },
            ],
        );
        let files = provider.scan_file_path(&ctx.state()).await.unwrap();

        assert_eq!(
            sorted_files(files),
            vec![
                (
                    "s3://warehouse/hive/table/dt=2024-01-01/file1.parquet".to_string(),
                    1,
                ),
                (
                    "s3://warehouse/hive/table/dt=2024-01-02/file2.parquet".to_string(),
                    2,
                ),
            ]
        );
    }

    #[tokio::test]
    async fn test_scan_file_path_metadata_filters_hidden_and_empty_files() {
        let ctx = session_context_with_store();
        let store = test_store(&ctx);
        put_test_object(&store, "hive/table/file1.parquet", b"1").await;
        put_test_object(&store, "hive/table/_temporary", b"1").await;
        put_test_object(&store, "hive/table/.metadata", b"1").await;
        put_test_object(&store, "hive/table/empty.parquet", b"").await;

        let provider = test_provider("s3://warehouse/hive/table", vec![]);
        let files = provider.scan_file_path(&ctx.state()).await.unwrap();

        assert_eq!(
            sorted_files(files),
            vec![("s3://warehouse/hive/table/file1.parquet".to_string(), 1)]
        );
    }

    #[tokio::test]
    async fn test_file_path_metadata_scan_applies_projection_and_limit() {
        let ctx = session_context_with_store();
        let store = test_store(&ctx);
        put_test_object(&store, "hive/table/file1.parquet", b"123").await;
        put_test_object(&store, "hive/table/file2.parquet", b"12345").await;

        let provider = test_provider("s3://warehouse/hive/table", vec![]);
        let projection = vec![1];
        let plan = provider
            .scan(&ctx.state(), Some(&projection), &[], Some(1))
            .await
            .unwrap();
        let batches = collect(plan, ctx.task_ctx()).await.unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        assert_eq!(batches[0].num_columns(), 1);
        assert_eq!(batches[0].schema().field(0).name(), "file_size");
        assert!(!batches[0].column(0).is_null(0));
    }

    fn session_context_with_store() -> SessionContext {
        let ctx = SessionContext::new();
        ctx.runtime_env().register_object_store(
            &Url::parse("s3://warehouse").unwrap(),
            Arc::new(InMemory::new()),
        );
        ctx
    }

    fn test_store(ctx: &SessionContext) -> Arc<dyn ObjectStore> {
        ctx.runtime_env()
            .object_store(ObjectStoreUrl::parse("s3://warehouse").unwrap())
            .unwrap()
    }

    fn test_provider(
        table_location: &str,
        partitions: Vec<HivePartition>,
    ) -> HiveMetadataTableProvider {
        HiveMetadataTableProvider::try_new(
            test_storage_info(table_location),
            partitions,
            MetadataTableType::FilePath,
            None,
        )
        .unwrap()
    }

    fn test_storage_info(table_location: &str) -> HiveStorageInfo {
        HiveStorageInfo {
            table_location: table_location.to_string(),
            input_format: crate::table_format::hive::hive_storage_info::HiveInputFormat::Parquet,
            table_schema: TableSchema::new(Arc::new(Schema::empty()), vec![]),
            serde_properties: HashMap::new(),
            table_properties: HashMap::new(),
        }
    }

    async fn put_test_object(store: &Arc<dyn ObjectStore>, path: &str, data: &[u8]) {
        store
            .put(&Path::from(path), data.to_vec().into())
            .await
            .unwrap();
    }

    fn sorted_files(files: Vec<HiveFileMetadata>) -> Vec<(String, u64)> {
        let mut files = files
            .into_iter()
            .map(|file| (file.path, file.size))
            .collect::<Vec<_>>();
        files.sort();
        files
    }
}
