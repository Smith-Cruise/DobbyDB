use crate::storage::parse_location_schema_host;
use datafusion::arrow::datatypes::Schema;
use datafusion::catalog::memory::DataSourceExec;
use datafusion::common::Result;
use datafusion::config::{ConfigOptions, TableParquetOptions};
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfigBuilder, ParquetSource};
use datafusion::error::DataFusionError;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::StreamExt;
use iceberg::spec::DataFileFormat;
use iceberg::table::Table;
use iceberg_datafusion::to_datafusion_error;
use std::any::Any;
use std::fmt::Formatter;
use std::sync::Arc;

pub struct IcebergTableScanBuilder<'a> {
    table: Table,
    schema: Arc<Schema>,
    snapshot_id: Option<i64>,
    projection: Option<&'a Vec<usize>>,
    filters: Option<&'a [Expr]>,
}

impl<'a> IcebergTableScanBuilder<'a> {
    pub fn new(table: Table, schema: Arc<Schema>) -> Self {
        Self {
            table,
            schema,
            snapshot_id: None,
            projection: None,
            filters: None,
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

        // let predicates = convert_filters_to_predicate(self.filters)?;
        // iceberg_table_scan_builder.with_filter()

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

        // let scan_config_builder = FileScanConfigBuilder::new();
        let parquet_options = TableParquetOptions::default();
        let file_source = ParquetSource::new(parquet_options);

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
            Arc::new(file_source),
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
}
