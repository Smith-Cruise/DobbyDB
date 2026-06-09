use crate::table_format::hive::hive_file_utils::{
    list_files_by_directories, list_files_by_directories_grouped,
};
use crate::table_format::hive::hive_partition::HivePartition;
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
use datafusion::logical_expr::Expr;
use datafusion::object_store::{ObjectMeta, ObjectStore};
use datafusion::physical_plan::ExecutionPlan;
use dobbydb_storage::storage::{
    Storage, parse_location_schema_authority, try_register_storage_info_session,
};
use std::any::Any;
use std::sync::Arc;
use url::Url;

#[derive(Debug)]
pub struct HiveDataFilesMetadataTableProvider {
    table_location: String,
    partitions: Vec<HivePartition>,
    storage: Option<Storage>,
    schema: SchemaRef,
}

impl HiveDataFilesMetadataTableProvider {
    pub fn new(
        table_location: String,
        partitions: Vec<HivePartition>,
        storage: Option<Storage>,
    ) -> Self {
        Self {
            table_location,
            partitions,
            storage,
            schema: Self::schema(),
        }
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("file_path", DataType::Utf8, false),
            Field::new("file_size", DataType::UInt64, false),
        ]))
    }

    fn convert_object_meta_to_record_batch(
        schema: SchemaRef,
        table_location: &str,
        files: Vec<ObjectMeta>,
    ) -> Result<RecordBatch> {
        let full_paths = files
            .iter()
            .map(|file| object_meta_full_location(table_location, file))
            .collect::<Result<Vec<_>>>()?;
        let file_paths =
            StringArray::from(full_paths.iter().map(|p| p.as_str()).collect::<Vec<_>>());
        let file_sizes = UInt64Array::from(files.iter().map(|file| file.size).collect::<Vec<_>>());

        RecordBatch::try_new(schema, vec![Arc::new(file_paths), Arc::new(file_sizes)])
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
    }

    async fn retrieve_table_data_files(&self, state: &dyn Session) -> Result<Vec<ObjectMeta>> {
        let object_store = get_object_store(state, self.storage.as_ref(), &self.table_location)?;

        let dir_locations = if self.partitions.is_empty() {
            vec![self.table_location.clone()]
        } else {
            self.partitions
                .iter()
                .map(|partition| partition.location.clone())
                .collect()
        };

        list_files_by_directories(state, &object_store, dir_locations).await
    }
}

#[async_trait]
impl TableProvider for HiveDataFilesMetadataTableProvider {
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
        let schema = self.schema();
        let mut files = self.retrieve_table_data_files(state).await?;
        if let Some(limit) = limit {
            files.truncate(limit);
        }
        let batch = Self::convert_object_meta_to_record_batch(
            Arc::clone(&schema),
            &self.table_location,
            files,
        )?;
        let source = MemorySourceConfig::try_new(&[vec![batch]], schema, projection.cloned())?;
        Ok(DataSourceExec::from_data_source(source.with_limit(limit)))
    }
}

#[derive(Debug)]
pub struct HivePartitionsMetadataTableProvider {
    table_location: String,
    partition_fields: Vec<Arc<Field>>,
    partitions: Vec<HivePartition>,
    storage: Option<Storage>,
    schema: SchemaRef,
}

impl HivePartitionsMetadataTableProvider {
    pub fn new(
        table_location: String,
        partition_fields: Vec<Arc<Field>>,
        partitions: Vec<HivePartition>,
        storage: Option<Storage>,
    ) -> Self {
        Self {
            table_location,
            partition_fields,
            partitions,
            storage,
            schema: Self::schema(),
        }
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("partition", DataType::Utf8, false),
            Field::new("data_file_count", DataType::UInt64, false),
            Field::new("total_data_file_size", DataType::UInt64, false),
        ]))
    }

    fn convert_partitions_to_record_batch(
        schema: SchemaRef,
        partition_fields: &[Arc<Field>],
        partitions: &[HivePartition],
        files: Vec<Vec<ObjectMeta>>,
    ) -> Result<RecordBatch> {
        if partitions.len() != files.len() {
            return Err(DataFusionError::Internal(format!(
                "partition and file group counts differ: {} != {}",
                partitions.len(),
                files.len()
            )));
        }

        let partition_names = partitions
            .iter()
            .map(|partition| Self::format_partition_name(partition_fields, partition))
            .collect::<Result<Vec<_>>>()?;
        let data_file_counts = files
            .iter()
            .map(|partition_files| partition_files.len() as u64)
            .collect::<Vec<_>>();
        let total_data_file_sizes = files
            .iter()
            .map(|partition_files| partition_files.iter().map(|file| file.size).sum::<u64>())
            .collect::<Vec<_>>();

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(partition_names)),
                Arc::new(UInt64Array::from(data_file_counts)),
                Arc::new(UInt64Array::from(total_data_file_sizes)),
            ],
        )
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
    }

    fn format_partition_name(
        partition_fields: &[Arc<Field>],
        partition: &HivePartition,
    ) -> Result<String> {
        if partition_fields.len() != partition.partition_values.len() {
            return Err(DataFusionError::Internal(format!(
                "partition field and value counts differ: {} != {}",
                partition_fields.len(),
                partition.partition_values.len()
            )));
        }

        Ok(partition_fields
            .iter()
            .zip(&partition.partition_values)
            .map(|(field, value)| format!("{}={}", field.name(), value))
            .collect::<Vec<_>>()
            .join("/"))
    }

    async fn retrieve_partition_data_files(
        &self,
        state: &dyn Session,
        partitions: &[HivePartition],
    ) -> Result<Vec<Vec<ObjectMeta>>> {
        let object_store = get_object_store(state, self.storage.as_ref(), &self.table_location)?;
        let dir_locations = partitions
            .iter()
            .map(|partition| partition.location.clone())
            .collect();

        list_files_by_directories_grouped(state, &object_store, dir_locations).await
    }
}

#[async_trait]
impl TableProvider for HivePartitionsMetadataTableProvider {
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
        let schema = self.schema();
        let partitions = match limit {
            Some(limit) => &self.partitions[..self.partitions.len().min(limit)],
            None => &self.partitions,
        };
        let files = self
            .retrieve_partition_data_files(state, partitions)
            .await?;
        let batch = Self::convert_partitions_to_record_batch(
            Arc::clone(&schema),
            &self.partition_fields,
            partitions,
            files,
        )?;
        let source = MemorySourceConfig::try_new(&[vec![batch]], schema, projection.cloned())?;
        Ok(DataSourceExec::from_data_source(source.with_limit(limit)))
    }
}

fn get_object_store(
    state: &dyn Session,
    storage: Option<&Storage>,
    table_location: &str,
) -> Result<Arc<dyn ObjectStore>> {
    try_register_storage_info_session(storage, table_location, state)?;

    let (path_schema, path_bucket) = parse_location_schema_authority(table_location)?;
    let store_url = ObjectStoreUrl::parse(format!("{}://{}", path_schema, path_bucket))?;
    state.runtime_env().object_store(&store_url)
}

fn object_meta_full_location(table_location: &str, file: &ObjectMeta) -> Result<String> {
    let parsed = Url::parse(table_location).map_err(|e| DataFusionError::External(e.into()))?;
    let authority = match parsed.authority() {
        "" => format!("{}://", parsed.scheme()),
        authority => format!("{}://{}", parsed.scheme(), authority),
    };

    Ok(format!(
        "{}/{}",
        authority.trim_end_matches('/'),
        file.location.as_ref().trim_start_matches('/')
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Array;
    use datafusion::object_store::ObjectStore;
    use datafusion::object_store::ObjectStoreExt;
    use datafusion::object_store::memory::InMemory;
    use datafusion::object_store::path::Path;
    use datafusion::physical_plan::collect;
    use datafusion::prelude::SessionContext;

    #[tokio::test]
    async fn test_scan_data_files_metadata_for_unpartitioned_table() {
        let ctx = session_context_with_store();
        let store = build_test_object_store(&ctx);
        put_test_object(&store, "hive/table/file1.parquet", b"123").await;
        put_test_object(&store, "hive/table/file2.parquet", b"12345").await;

        let provider = build_test_hive_table_metadata_provider("s3://warehouse/hive/table", vec![]);
        let files = provider
            .retrieve_table_data_files(&ctx.state())
            .await
            .unwrap();

        assert_eq!(
            sorted_files(&files),
            vec![
                ("hive/table/file1.parquet", 3),
                ("hive/table/file2.parquet", 5),
            ]
        );
    }

    #[tokio::test]
    async fn test_scan_data_files_metadata_for_partitioned_table() {
        let ctx = session_context_with_store();
        let store = build_test_object_store(&ctx);
        put_test_object(&store, "hive/table/dt=2024-01-01/file1.parquet", b"1").await;
        put_test_object(&store, "hive/table/dt=2024-01-02/file2.parquet", b"22").await;

        let provider = build_test_hive_table_metadata_provider(
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
        let files = provider
            .retrieve_table_data_files(&ctx.state())
            .await
            .unwrap();

        assert_eq!(
            sorted_files(&files),
            vec![
                ("hive/table/dt=2024-01-01/file1.parquet", 1),
                ("hive/table/dt=2024-01-02/file2.parquet", 2),
            ]
        );
    }

    #[tokio::test]
    async fn test_scan_data_files_metadata_filters_hidden_and_empty_files() {
        let ctx = session_context_with_store();
        let store = build_test_object_store(&ctx);
        put_test_object(&store, "hive/table/file1.parquet", b"1").await;
        put_test_object(&store, "hive/table/_temporary", b"1").await;
        put_test_object(&store, "hive/table/.metadata", b"1").await;
        put_test_object(&store, "hive/table/empty.parquet", b"").await;

        let provider = build_test_hive_table_metadata_provider("s3://warehouse/hive/table", vec![]);
        let files = provider
            .retrieve_table_data_files(&ctx.state())
            .await
            .unwrap();

        assert_eq!(sorted_files(&files), vec![("hive/table/file1.parquet", 1)]);
    }

    #[tokio::test]
    async fn test_data_files_metadata_scan_applies_projection_and_limit() {
        let ctx = session_context_with_store();
        let store = build_test_object_store(&ctx);
        put_test_object(&store, "hive/table/file1.parquet", b"123").await;
        put_test_object(&store, "hive/table/file2.parquet", b"12345").await;

        let provider = build_test_hive_table_metadata_provider("s3://warehouse/hive/table", vec![]);
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

    #[tokio::test]
    async fn test_partitions_metadata_for_unpartitioned_table_returns_empty_batch() {
        let ctx = session_context_with_store();
        let store = build_test_object_store(&ctx);
        put_test_object(&store, "hive/table/file1.parquet", b"123").await;

        let provider = build_test_hive_partitions_metadata_provider(vec![], vec![]);
        let plan = provider.scan(&ctx.state(), None, &[], None).await.unwrap();
        let batches = collect(plan, ctx.task_ctx()).await.unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 0);
        assert_eq!(batches[0].num_columns(), 3);
        assert_eq!(batches[0].schema().field(0).name(), "partition");
        assert_eq!(batches[0].schema().field(1).name(), "data_file_count");
        assert_eq!(batches[0].schema().field(2).name(), "total_data_file_size");
    }

    #[tokio::test]
    async fn test_partitions_metadata_aggregates_visible_non_empty_files() {
        let ctx = session_context_with_store();
        let store = build_test_object_store(&ctx);
        put_test_object(
            &store,
            "hive/table/dt=2026-01-01/country=CN/file1.parquet",
            b"123",
        )
        .await;
        put_test_object(
            &store,
            "hive/table/dt=2026-01-01/country=CN/file2.parquet",
            b"12345",
        )
        .await;
        put_test_object(
            &store,
            "hive/table/dt=2026-01-01/country=CN/_temporary",
            b"ignored",
        )
        .await;
        put_test_object(
            &store,
            "hive/table/dt=2026-01-01/country=CN/empty.parquet",
            b"",
        )
        .await;

        let partition_fields = vec![
            Arc::new(Field::new("dt", DataType::Date32, true)),
            Arc::new(Field::new("country", DataType::Utf8, true)),
        ];
        let partitions = vec![
            HivePartition {
                location: "s3://warehouse/hive/table/dt=2026-01-01/country=CN".to_string(),
                partition_values: vec!["2026-01-01".to_string(), "CN".to_string()],
            },
            HivePartition {
                location: "s3://warehouse/hive/table/dt=2026-01-02/country=US".to_string(),
                partition_values: vec!["2026-01-02".to_string(), "US".to_string()],
            },
        ];
        let provider = build_test_hive_partitions_metadata_provider(partition_fields, partitions);
        let plan = provider.scan(&ctx.state(), None, &[], None).await.unwrap();
        let batches = collect(plan, ctx.task_ctx()).await.unwrap();
        let batch = &batches[0];

        let partition_names = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let data_file_counts = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let total_data_file_sizes = batch
            .column(2)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();

        assert_eq!(batch.num_rows(), 2);
        assert_eq!(partition_names.value(0), "dt=2026-01-01/country=CN");
        assert_eq!(data_file_counts.value(0), 2);
        assert_eq!(total_data_file_sizes.value(0), 8);
        assert_eq!(partition_names.value(1), "dt=2026-01-02/country=US");
        assert_eq!(data_file_counts.value(1), 0);
        assert_eq!(total_data_file_sizes.value(1), 0);
    }

    #[tokio::test]
    async fn test_partitions_metadata_scan_applies_projection_and_limit() {
        let ctx = session_context_with_store();
        let partition_fields = vec![Arc::new(Field::new("dt", DataType::Utf8, true))];
        let partitions = vec![
            HivePartition {
                location: "s3://warehouse/hive/table/dt=2026-01-01".to_string(),
                partition_values: vec!["2026-01-01".to_string()],
            },
            HivePartition {
                location: "s3://warehouse/hive/table/dt=2026-01-02".to_string(),
                partition_values: vec!["2026-01-02".to_string()],
            },
        ];
        let provider = build_test_hive_partitions_metadata_provider(partition_fields, partitions);
        let projection = vec![0];
        let plan = provider
            .scan(&ctx.state(), Some(&projection), &[], Some(1))
            .await
            .unwrap();
        let batches = collect(plan, ctx.task_ctx()).await.unwrap();

        assert_eq!(batches[0].num_rows(), 1);
        assert_eq!(batches[0].num_columns(), 1);
        assert_eq!(batches[0].schema().field(0).name(), "partition");
    }

    // mainly test about host:port
    #[tokio::test]
    async fn test_object_meta_full_location_preserves_hdfs_authority_port() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        put_test_object(&store, "hive/table/file1.parquet", b"123").await;
        let file = store
            .head(&Path::from("hive/table/file1.parquet"))
            .await
            .unwrap();

        let full_path =
            object_meta_full_location("hdfs://namenode:8020/hive/table", &file).unwrap();

        assert_eq!(full_path, "hdfs://namenode:8020/hive/table/file1.parquet");
    }

    fn session_context_with_store() -> SessionContext {
        let ctx = SessionContext::new();
        ctx.runtime_env().register_object_store(
            &Url::parse("s3://warehouse").unwrap(),
            Arc::new(InMemory::new()),
        );
        ctx
    }

    fn build_test_object_store(ctx: &SessionContext) -> Arc<dyn ObjectStore> {
        ctx.runtime_env()
            .object_store(ObjectStoreUrl::parse("s3://warehouse").unwrap())
            .unwrap()
    }

    fn build_test_hive_table_metadata_provider(
        table_location: &str,
        partitions: Vec<HivePartition>,
    ) -> HiveDataFilesMetadataTableProvider {
        HiveDataFilesMetadataTableProvider::new(table_location.to_string(), partitions, None)
    }

    fn build_test_hive_partitions_metadata_provider(
        partition_fields: Vec<Arc<Field>>,
        partitions: Vec<HivePartition>,
    ) -> HivePartitionsMetadataTableProvider {
        HivePartitionsMetadataTableProvider::new(
            "s3://warehouse/hive/table".to_string(),
            partition_fields,
            partitions,
            None,
        )
    }

    async fn put_test_object(store: &Arc<dyn ObjectStore>, path: &str, data: &[u8]) {
        store
            .put(&Path::from(path), data.to_vec().into())
            .await
            .unwrap();
    }

    fn sorted_files(files: &[ObjectMeta]) -> Vec<(&str, u64)> {
        let mut result: Vec<_> = files
            .iter()
            .map(|file| (file.location.as_ref(), file.size))
            .collect();
        result.sort();
        result
    }
}
