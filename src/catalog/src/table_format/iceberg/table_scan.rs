use crate::storage::parse_location_schema_host;
use crate::table_format::iceberg::expr_to_predicate::convert_filters_to_predicate;
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::catalog::memory::DataSourceExec;
use datafusion::common::Result;
use datafusion::config::{ConfigOptions, TableParquetOptions};
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{
    FileGroup, FileScanConfigBuilder, FileSource, ParquetSource,
};
use datafusion::datasource::schema_adapter::{
    DefaultSchemaAdapterFactory, SchemaAdapter, SchemaAdapterFactory, SchemaMapper,
};
use datafusion::error::DataFusionError;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::Expr;
use datafusion::parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::StreamExt;
use iceberg::spec::DataFileFormat;
use iceberg::table::Table;
use iceberg_datafusion::to_datafusion_error;
use std::any::Any;
use std::collections::HashMap;
use std::fmt::Formatter;
use std::sync::Arc;

pub struct IcebergTableScanBuilder<'a> {
    table: Table,
    schema: Arc<Schema>,
    snapshot_id: Option<i64>,
    projection: Option<&'a Vec<usize>>,
    filters: Option<&'a [Expr]>,
    parquet_predicate: Option<Arc<dyn PhysicalExpr>>,
}

impl<'a> IcebergTableScanBuilder<'a> {
    pub fn new(table: Table, schema: Arc<Schema>) -> Self {
        Self {
            table,
            schema,
            snapshot_id: None,
            projection: None,
            filters: None,
            parquet_predicate: None,
        }
    }

    pub fn with_snapshot_id(mut self, snapshot_id: Option<i64>) -> Self {
        self.snapshot_id = snapshot_id;
        self
    }

    pub fn with_projection(mut self, projection: Option<&'a Vec<usize>>) -> Self {
        self.projection = projection;
        self
    }

    pub fn with_filters(mut self, filters: Option<&'a [Expr]>) -> Self {
        self.filters = filters;
        self
    }

    pub fn with_predicate(mut self, parquet_predicate: Option<Arc<dyn PhysicalExpr>>) -> Self {
        self.parquet_predicate = parquet_predicate;
        self
    }

    pub async fn build(&self) -> Result<IcebergTableScan> {
        let mut iceberg_table_scan_builder = match self.snapshot_id {
            Some(snapshot_id) => self.table.scan().snapshot_id(snapshot_id),
            None => self.table.scan(),
        };

        let projection_column_names = get_column_names(self.schema.clone(), self.projection);

        iceberg_table_scan_builder = match projection_column_names {
            Some(column_names) => iceberg_table_scan_builder.select(column_names),
            None => iceberg_table_scan_builder.select_all(),
        };

        let iceberg_predicates = match self.filters {
            Some(filters) => convert_filters_to_predicate(filters),
            None => None,
        };

        if let Some(predicates) = iceberg_predicates {
            iceberg_table_scan_builder = iceberg_table_scan_builder.with_filter(predicates)
        }

        let mut iceberg_file_scan_tasks = iceberg_table_scan_builder
            .build()
            .map_err(to_datafusion_error)?
            .plan_files()
            .await
            .map_err(to_datafusion_error)?;

        let mut partition_fields: Vec<PartitionedFile> = Vec::new();

        let metadata_location = match self.table.metadata_location() {
            Some(metadata_location) => metadata_location,
            None => {
                return Err(DataFusionError::Internal(
                    "empty metadata location".to_string(),
                ))?;
            }
        };

        let (path_schema, path_host) = parse_location_schema_host(metadata_location)?;

        let data_file_truncate = IcebergDataFilePathTruncate::try_new(metadata_location)?;

        while let Some(file_scan_task) = iceberg_file_scan_tasks.next().await {
            match file_scan_task {
                Ok(file_scan_task) => {
                    if file_scan_task.start != 0 {
                        return Err(DataFusionError::NotImplemented(String::from(
                            "iceberg file scan task must start with offset=0",
                        )));
                    }

                    if file_scan_task.data_file_format != DataFileFormat::Parquet {
                        return Err(DataFusionError::NotImplemented(String::from(
                            "iceberg only support parquet files",
                        )));
                    }

                    if !file_scan_task.deletes.is_empty() {
                        return Err(DataFusionError::NotImplemented(String::from(
                            "iceberg file with delete files is not supported",
                        )));
                    }

                    let partition_field = PartitionedFile::new(
                        data_file_truncate.truncate(&file_scan_task.data_file_path)?,
                        file_scan_task.length,
                    );
                    partition_fields.push(partition_field);
                }
                Err(e) => {
                    return Err(to_datafusion_error(e));
                }
            }
        }

        let mut parquet_options = TableParquetOptions::default();
        parquet_options.global.pushdown_filters = true;
        parquet_options.global.reorder_filters = true;
        let mut file_source = ParquetSource::new(parquet_options);
        if let Some(physical_predicate) = &self.parquet_predicate {
            file_source = file_source.with_predicate(Arc::clone(physical_predicate));
        }
        let file_source = file_source.with_schema_adapter_factory(Arc::new(
            IcebergFieldIdSchemaAdapterFactory
        ))?;

        // 根据 projection 创建投影后的 schema
        let projected_schema = if let Some(projection) = self.projection {
            let projected_fields: Vec<_> = projection
                .iter()
                .map(|&i| self.schema.field(i).clone())
                .collect();
            Arc::new(Schema::new(projected_fields))
        } else {
            self.schema.clone()
        };

        let file_scan_config = FileScanConfigBuilder::new(
            ObjectStoreUrl::parse(format!("{}://{}", path_schema, path_host))?,
            projected_schema,
            file_source,
        )
        .with_file_group(FileGroup::new(partition_fields))
        .build();

        Ok(IcebergTableScan::new(DataSourceExec::from_data_source(
            file_scan_config,
        )))
    }
}

fn get_column_names(schema: Arc<Schema>, projection: Option<&Vec<usize>>) -> Option<Vec<String>> {
    projection.map(|v| {
        v.iter()
            .map(|p| schema.field(*p).name().clone())
            .collect::<Vec<String>>()
    })
}

#[derive(Debug, Default)]
struct IcebergFieldIdSchemaAdapterFactory;

impl SchemaAdapterFactory for IcebergFieldIdSchemaAdapterFactory {
    fn create(
        &self,
        projected_table_schema: SchemaRef,
        _table_schema: SchemaRef,
    ) -> Box<dyn SchemaAdapter> {
        Box::new(IcebergFieldIdSchemaAdapter {
            projected_table_schema,
        })
    }
}

#[derive(Debug)]
struct IcebergFieldIdSchemaAdapter {
    projected_table_schema: SchemaRef,
}

impl IcebergFieldIdSchemaAdapter {
    fn get_field_id(field: &Field) -> Result<Option<i32>> {
        match field.metadata().get(PARQUET_FIELD_ID_META_KEY) {
            Some(raw_value) => raw_value.parse::<i32>().map(Some).map_err(|e| {
                DataFusionError::Execution(format!(
                    "failed to parse `{}` metadata `{}` to i32 for field `{}`: {}",
                    PARQUET_FIELD_ID_META_KEY,
                    raw_value,
                    field.name(),
                    e
                ))
            }),
            None => Ok(None),
        }
    }

    fn table_field_id_to_name(&self) -> Result<HashMap<i32, String>> {
        let mut field_id_to_name = HashMap::with_capacity(self.projected_table_schema.fields.len());
        for field in &self.projected_table_schema.fields {
            let field_id = Self::get_field_id(field)?.ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "projected table field `{}` is missing `{}` metadata",
                    field.name(),
                    PARQUET_FIELD_ID_META_KEY
                ))
            })?;
            field_id_to_name.insert(field_id, field.name().clone());
        }
        Ok(field_id_to_name)
    }
}

impl SchemaAdapter for IcebergFieldIdSchemaAdapter {
    fn map_column_index(&self, index: usize, file_schema: &Schema) -> Option<usize> {
        let table_field = self.projected_table_schema.field(index);
        let table_field_id = Self::get_field_id(table_field).ok().flatten()?;

        for (file_idx, file_field) in file_schema.fields.iter().enumerate() {
            if Self::get_field_id(file_field).ok().flatten() == Some(table_field_id) {
                return Some(file_idx);
            }
        }
        None
    }

    fn map_schema(
        &self,
        file_schema: &Schema,
    ) -> Result<(Arc<dyn SchemaMapper>, Vec<usize>)> {
        let table_field_id_to_name = self.table_field_id_to_name()?;
        let mut seen_file_field_ids: HashMap<i32, String> = HashMap::new();
        let mut remapped_file_fields = Vec::with_capacity(file_schema.fields.len());

        for file_field in &file_schema.fields {
            let mut remapped_field = file_field.as_ref().clone();
            if let Some(file_field_id) = Self::get_field_id(file_field)? {
                if let Some(existing_name) =
                    seen_file_field_ids.insert(file_field_id, file_field.name().clone())
                {
                    return Err(DataFusionError::Execution(format!(
                        "duplicate field_id {} found in file schema for fields `{}` and `{}`",
                        file_field_id,
                        existing_name,
                        file_field.name()
                    )));
                }
                if let Some(table_field_name) = table_field_id_to_name.get(&file_field_id) {
                    remapped_field = remapped_field.with_name(table_field_name.clone());
                }
            }
            remapped_file_fields.push(remapped_field);
        }

        let remapped_file_schema = Schema::new(remapped_file_fields);
        let delegate =
            DefaultSchemaAdapterFactory.create_with_projected_schema(self.projected_table_schema.clone());
        delegate.map_schema(&remapped_file_schema)
    }
}

struct IcebergDataFilePathTruncate {
    base_path: String,
}

impl IcebergDataFilePathTruncate {
    fn try_new(path: &str) -> Result<Self> {
        let (schema, host) = parse_location_schema_host(path)?;
        Ok(IcebergDataFilePathTruncate {
            base_path: format!("{}://{}", schema, host),
        })
    }

    fn truncate(&self, input_path: &str) -> Result<String> {
        match input_path.strip_prefix(&self.base_path) {
            Some(extract_path) => Ok(extract_path.to_string()),
            None => Err(DataFusionError::Internal(format!(
                "failed to truncate path, base_path: {}, input_path: {}",
                self.base_path, input_path
            ))),
        }
    }
}

#[derive(Debug, Clone)]
pub struct IcebergTableScan {
    parquet_scan: Arc<dyn ExecutionPlan>,
}

impl IcebergTableScan {
    pub fn new(parquet_scan: Arc<dyn ExecutionPlan>) -> Self {
        IcebergTableScan { parquet_scan }
    }
}

impl ExecutionPlan for IcebergTableScan {
    fn name(&self) -> &str {
        "IcebergTableScan"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        self.parquet_scan.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.parquet_scan]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Plan(format!(
                "wrong number of children {}",
                children.len()
            )));
        }
        Ok(Arc::new(IcebergTableScan::new(children[0].clone())))
    }

    fn repartitioned(
        &self,
        target_partitions: usize,
        config: &ConfigOptions,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        if let Some(partition_scan) = self.parquet_scan.repartitioned(target_partitions, config)? {
            Ok(Some(Arc::new(IcebergTableScan::new(partition_scan))))
        } else {
            Ok(None)
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.parquet_scan.execute(partition, context)
    }
}

impl DisplayAs for IcebergTableScan {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "IcebergTableScan")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Array, Int32Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field};
    use std::collections::HashMap;

    fn field_with_id(name: &str, data_type: DataType, field_id: i32) -> Field {
        Field::new(name, data_type, true).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            field_id.to_string(),
        )]))
    }

    #[test]
    fn test_iceberg_data_file_path_truncate() {
        let truncate =
            IcebergDataFilePathTruncate::try_new("s3://warehouse/hive/1.parquet").unwrap();
        assert_eq!(
            "/hive/1.parquet",
            truncate.truncate("s3://warehouse/hive/1.parquet").unwrap()
        );
        assert_eq!(
            "/hive/2.parquet",
            truncate.truncate("s3://warehouse/hive/2.parquet").unwrap()
        );
    }

    #[test]
    fn test_field_id_rename_maps_correctly() {
        let adapter = IcebergFieldIdSchemaAdapter {
            projected_table_schema: Arc::new(Schema::new(vec![
                field_with_id("new_name", DataType::Int32, 2),
                field_with_id("qty", DataType::Int32, 3),
            ])),
        };

        let file_schema = Schema::new(vec![
            field_with_id("old_name", DataType::Int32, 2),
            field_with_id("qty", DataType::Int32, 3),
        ]);

        assert_eq!(Some(0), adapter.map_column_index(0, &file_schema));
        let (mapper, projection) = adapter.map_schema(&file_schema).unwrap();
        assert_eq!(vec![0, 1], projection);

        let batch = RecordBatch::try_new(
            Arc::new(file_schema),
            vec![
                Arc::new(Int32Array::from(vec![10, 20])),
                Arc::new(Int32Array::from(vec![1, 2])),
            ],
        )
        .unwrap();

        let mapped = mapper.map_batch(batch).unwrap();
        assert_eq!(2, mapped.num_columns());
        assert_eq!("new_name", mapped.schema().field(0).name());
        assert_eq!(
            vec![10, 20],
            mapped
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .to_vec()
        );
    }

    #[test]
    fn test_missing_field_id_in_file_results_in_null_column() {
        let adapter = IcebergFieldIdSchemaAdapter {
            projected_table_schema: Arc::new(Schema::new(vec![
                field_with_id("id", DataType::Int32, 1),
                field_with_id("extra", DataType::Int32, 2),
            ])),
        };
        let file_schema = Schema::new(vec![field_with_id("id", DataType::Int32, 1)]);

        assert_eq!(None, adapter.map_column_index(1, &file_schema));
        let (mapper, projection) = adapter.map_schema(&file_schema).unwrap();
        assert_eq!(vec![0], projection);

        let batch = RecordBatch::try_new(
            Arc::new(file_schema),
            vec![Arc::new(Int32Array::from(vec![1, 2]))],
        )
        .unwrap();
        let mapped = mapper.map_batch(batch).unwrap();

        let extra = mapped.column(1).as_any().downcast_ref::<Int32Array>().unwrap();
        assert!(extra.is_null(0));
        assert!(extra.is_null(1));
    }

    #[test]
    fn test_missing_field_id_in_table_schema_returns_error() {
        let adapter = IcebergFieldIdSchemaAdapter {
            projected_table_schema: Arc::new(Schema::new(vec![Field::new(
                "id",
                DataType::Int32,
                true,
            )])),
        };
        let file_schema = Schema::new(vec![field_with_id("id", DataType::Int32, 1)]);
        let err = adapter.map_schema(&file_schema).unwrap_err();
        assert!(err
            .to_string()
            .contains("is missing `PARQUET:field_id` metadata"));
    }
}
