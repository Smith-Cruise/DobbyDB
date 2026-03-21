use crate::table_format::hive::HiveStorageInfo;
use crate::table_format::hive::hive_partition::HivePartition;
use crate::table_format::hive::hive_storage_info::HiveInputFormat;
use async_trait::async_trait;
use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Float32Array, Float64Array, Int8Array, Int16Array,
    Int32Array, Int64Array, StringArray, TimestampMicrosecondArray,
};
use datafusion::arrow::compute;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::memory::DataSourceExec;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::config::CsvOptions;
use datafusion::common::parsers::CompressionTypeVariant;
use datafusion::common::{Result, ToDFSchema};
use datafusion::config::TableParquetOptions;
use datafusion::datasource::TableType;
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{
    CsvSource, FileGroup, FileScanConfigBuilder, ParquetSource,
};
use datafusion::datasource::table_schema::TableSchema;
use datafusion::error::DataFusionError;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::utils::conjunction;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::object_store::path::Path;
use datafusion::object_store::{ObjectMeta, ObjectStore};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::scalar::ScalarValue;
use dobbydb_storage::storage::{Storage, parse_location_schema_bucket};
use futures::StreamExt;
use futures::stream;
use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::Arc;
use url::Url;

#[derive(Debug)]
pub struct HiveTableProvider {
    hive_storage_info: HiveStorageInfo,
    partitions: Vec<HivePartition>,
    storage: Option<Storage>,
}

impl HiveTableProvider {
    pub fn new(
        hive_storage_info: HiveStorageInfo,
        partitions: Vec<HivePartition>,
        storage: Option<Storage>,
    ) -> Self {
        Self {
            hive_storage_info,
            partitions,
            storage,
        }
    }
}

#[async_trait]
impl TableProvider for HiveTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.hive_storage_info.table_schema.table_schema().clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if let Some(storage) = &self.storage {
            storage.register_into_session(&self.hive_storage_info.table_location, state)?;
        }

        let (path_schema, path_bucket) =
            parse_location_schema_bucket(&self.hive_storage_info.table_location)?;
        let store_url = ObjectStoreUrl::parse(format!("{}://{}", path_schema, path_bucket))?;

        let object_store = state.runtime_env().object_store(&store_url)?;

        let scan_file_list: Vec<PartitionedFile> = if self.partitions.is_empty() {
            let file_object_metas =
                list_files(&object_store, &self.hive_storage_info.table_location).await?;
            file_object_metas
                .iter()
                .map(|file_object_meta| PartitionedFile::from(file_object_meta.clone()))
                .collect()
        } else {
            let selected_partition_indices = prune_partitions(
                &self.partitions,
                self.hive_storage_info.table_schema.table_partition_cols(),
                filters,
                state,
            )?;

            let meta_fetch_concurrency = state.config_options().execution.meta_fetch_concurrency;
            let partition_scan_tasks = selected_partition_indices
                .into_iter()
                .map(|partition_idx| {
                    let partition = &self.partitions[partition_idx];
                    let location = partition.location.clone();
                    let partition_values = build_partition_values(
                        self.hive_storage_info.table_schema.table_partition_cols(),
                        partition,
                    )?;
                    let object_store = Arc::clone(&object_store);

                    Ok(async move {
                        let file_object_metas = list_files(&object_store, &location).await?;
                        let partitioned_files = file_object_metas
                            .into_iter()
                            .map(|file_object_meta| {
                                let mut partitioned_file = PartitionedFile::from(file_object_meta);
                                partitioned_file.partition_values = partition_values.clone();
                                partitioned_file
                            })
                            .collect();

                        Ok(partitioned_files)
                    })
                })
                .collect::<Result<Vec<_>>>()?;

            collect_partitioned_files(partition_scan_tasks, meta_fetch_concurrency).await?
        };

        let file_group = FileGroup::new(scan_file_list);

        let exec = match &self.hive_storage_info.input_format {
            HiveInputFormat::TextFile => build_csv_exec(
                store_url,
                self.hive_storage_info.table_schema.clone(),
                file_group,
                &self.hive_storage_info.serde_properties,
                projection,
                limit,
            ),
            HiveInputFormat::Parquet => build_parquet_exec(
                store_url,
                self.hive_storage_info.table_schema.clone(),
                file_group,
                &filter_data_filters(filters, self.hive_storage_info.table_schema.file_schema()),
                state,
                self.hive_storage_info.table_schema.file_schema(),
                projection,
                limit,
            ),
            HiveInputFormat::ORC => {
                return Err(DataFusionError::NotImplemented(
                    "orc not implemented".to_string(),
                ));
            }
        }?;

        Ok(exec)
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }
}

fn build_csv_exec(
    store_url: ObjectStoreUrl,
    table_schema: TableSchema,
    file_group: FileGroup,
    serde_properties: &HashMap<String, String>,
    projection: Option<&Vec<usize>>,
    limit: Option<usize>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let delimiter = resolve_textfile_delimiter(serde_properties);
    let compression = detect_file_group_compression(&file_group)?;
    let file_compression = FileCompressionType::from(compression);

    let options = CsvOptions {
        has_header: Some(false),
        delimiter,
        compression,
        ..Default::default()
    };

    let source = Arc::new(CsvSource::new(table_schema).with_csv_options(options));
    let mut builder = FileScanConfigBuilder::new(store_url, source).with_file_group(file_group);
    builder = builder.with_file_compression_type(file_compression);
    if let Some(proj) = projection {
        builder = builder.with_projection_indices(Some(proj.clone()))?;
    }
    if let Some(lim) = limit {
        builder = builder.with_limit(Some(lim));
    }
    let config = builder.build();
    Ok(DataSourceExec::from_data_source(config))
}

fn build_partition_values(
    partition_fields: &[Arc<datafusion::arrow::datatypes::Field>],
    partition: &HivePartition,
) -> Result<Vec<ScalarValue>> {
    partition_fields
        .iter()
        .zip(partition.partition_values.iter())
        .map(|(field, val)| parse_partition_value(val, field.data_type()))
        .collect()
}

fn filter_data_filters(filters: &[Expr], data_schema: &SchemaRef) -> Vec<Expr> {
    if filters.is_empty() {
        return vec![];
    }

    let data_col_name_set: HashSet<String> = data_schema
        .fields()
        .iter()
        .map(|f| f.name().to_ascii_lowercase())
        .collect();

    filters
        .iter()
        .filter(|f| {
            f.column_refs()
                .iter()
                .all(|c| data_col_name_set.contains(&c.name().to_ascii_lowercase()))
        })
        .cloned()
        .collect()
}

fn resolve_textfile_delimiter(serde_properties: &HashMap<String, String>) -> u8 {
    [
        "field.delim",
        "serialization.format",
        "separatorChar",
        "serdeConstants.FIELD_DELIM",
        "columns.delimited.by",
    ]
    .iter()
    .find_map(|key| serde_properties.get(*key))
    .and_then(|raw| parse_hive_delimiter(raw))
    .unwrap_or(b'\x01')
}

fn parse_hive_delimiter(raw: &str) -> Option<u8> {
    let raw = raw.trim().trim_matches('\'').trim_matches('"');
    if raw.is_empty() {
        return None;
    }

    if raw.len() == 1 {
        return raw.as_bytes().first().copied();
    }

    if let Some(octal) = raw.strip_prefix('\\') {
        if octal.chars().all(|c| ('0'..='7').contains(&c)) {
            return u8::from_str_radix(octal, 8).ok();
        }

        return match octal {
            "t" => Some(b'\t'),
            "n" => Some(b'\n'),
            "r" => Some(b'\r'),
            _ => None,
        };
    }

    if let Some(hex) = raw.strip_prefix("0x") {
        return u8::from_str_radix(hex, 16).ok();
    }

    if let Some(hex) = raw.strip_prefix("\\u") {
        return u8::from_str_radix(hex, 16).ok();
    }

    raw.parse::<u8>().ok()
}

fn detect_file_group_compression(file_group: &FileGroup) -> Result<CompressionTypeVariant> {
    let mut compression_types = HashSet::new();
    for file in file_group.files() {
        let file_name = file.object_meta.location.filename().unwrap_or("");
        let compression = if file_name.ends_with(".gz") || file_name.ends_with(".gzip") {
            CompressionTypeVariant::GZIP
        } else if file_name.ends_with(".bz2") {
            CompressionTypeVariant::BZIP2
        } else if file_name.ends_with(".xz") {
            CompressionTypeVariant::XZ
        } else if file_name.ends_with(".zst") || file_name.ends_with(".zstd") {
            CompressionTypeVariant::ZSTD
        } else {
            CompressionTypeVariant::UNCOMPRESSED
        };
        compression_types.insert(compression);
    }

    if compression_types.len() <= 1 {
        return Ok(compression_types
            .into_iter()
            .next()
            .unwrap_or(CompressionTypeVariant::UNCOMPRESSED));
    }

    Err(DataFusionError::NotImplemented(
        "mixed compression in hive textfile scan is not supported".to_string(),
    ))
}

#[allow(clippy::too_many_arguments)]
fn build_parquet_exec(
    store_url: ObjectStoreUrl,
    table_schema: TableSchema,
    file_group: FileGroup,
    filters: &[Expr],
    state: &dyn Session,
    data_schema: &SchemaRef,
    projection: Option<&Vec<usize>>,
    limit: Option<usize>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let mut parquet_options = TableParquetOptions::default();
    parquet_options.global.pushdown_filters = true;
    parquet_options.global.reorder_filters = true;

    let mut source = ParquetSource::new(table_schema).with_table_parquet_options(parquet_options);

    let df_schema = data_schema.as_ref().clone().to_dfschema()?;
    let predicate = conjunction(filters.iter().cloned())
        .and_then(|p| state.create_physical_expr(p, &df_schema).ok());
    if let Some(pred) = predicate {
        source = source.with_predicate(pred);
    }

    let mut builder =
        FileScanConfigBuilder::new(store_url, Arc::new(source)).with_file_group(file_group);
    if let Some(proj) = projection {
        builder = builder.with_projection_indices(Some(proj.clone()))?;
    }
    if let Some(lim) = limit {
        builder = builder.with_limit(Some(lim));
    }
    let config = builder.build();
    Ok(DataSourceExec::from_data_source(config))
}

fn prune_partitions(
    partitions: &[HivePartition],
    partition_fields: &[Arc<Field>],
    filters: &[Expr],
    state: &dyn Session,
) -> Result<Vec<usize>> {
    if partition_fields.is_empty() {
        return Err(DataFusionError::Internal(
            "partition fields is empty".to_string(),
        ));
    }
    if filters.is_empty() {
        return Ok((0..partitions.len()).collect());
    }

    let partition_schema = Arc::new(Schema::new(
        partition_fields
            .iter()
            .map(|f| f.as_ref().clone())
            .collect::<Vec<_>>(),
    ));

    let partition_col_name_set: HashSet<String> = partition_schema
        .fields()
        .iter()
        .map(|field| field.name().to_ascii_lowercase())
        .collect();

    let partition_filters: Vec<&Expr> = filters
        .iter()
        .filter(|f| {
            !f.column_refs().is_empty()
                && f.column_refs()
                    .iter()
                    .all(|c| partition_col_name_set.contains(&c.name().to_ascii_lowercase()))
        })
        .collect();

    if partition_filters.is_empty() {
        return Ok((0..partitions.len()).collect());
    }

    let df_schema = partition_schema.as_ref().clone().to_dfschema()?;
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(partition_fields.len());
    for (field_idx, field) in partition_fields.iter().enumerate() {
        let values: Vec<Option<&str>> = partitions
            .iter()
            .map(|partition| {
                partition
                    .partition_values
                    .get(field_idx)
                    .map(|value| value.as_str())
            })
            .collect();
        columns.push(build_partition_array(field.data_type(), &values)?);
    }

    let batch = RecordBatch::try_new(partition_schema, columns)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
    let compiled_filters = partition_filters
        .into_iter()
        .map(|filter| state.create_physical_expr(filter.clone(), &df_schema))
        .collect::<Result<Vec<_>>>()?;

    let mut filter_result: Option<BooleanArray> = None;
    for filter in compiled_filters {
        let result_array = filter
            .evaluate(&batch)?
            .to_array_of_size(batch.num_rows())?;
        let bool_array = result_array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| {
                DataFusionError::Internal("partition filter did not produce boolean array".into())
            })?;
        filter_result = Some(match filter_result {
            None => bool_array.clone(),
            Some(acc) => compute::and(&acc, bool_array)
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?,
        });
    }

    let mut surviving = Vec::new();
    match filter_result {
        None => surviving.extend(0..partitions.len()),
        Some(combined_array) => {
            for row_idx in 0..partitions.len() {
                if !combined_array.is_null(row_idx) && combined_array.value(row_idx) {
                    surviving.push(row_idx);
                }
            }
        }
    }

    Ok(surviving)
}

fn build_partition_array(data_type: &DataType, values: &[Option<&str>]) -> Result<ArrayRef> {
    let normalized_values: Vec<Option<&str>> = values
        .iter()
        .map(|value| normalize_partition_value(*value))
        .collect();

    match data_type {
        DataType::Int8 => {
            let arr = Int8Array::from(
                normalized_values
                    .iter()
                    .map(|v| v.and_then(|s| s.parse::<i8>().ok()))
                    .collect::<Vec<_>>(),
            );
            Ok(Arc::new(arr) as ArrayRef)
        }
        DataType::Int16 => {
            let arr = Int16Array::from(
                normalized_values
                    .iter()
                    .map(|v| v.and_then(|s| s.parse::<i16>().ok()))
                    .collect::<Vec<_>>(),
            );
            Ok(Arc::new(arr) as ArrayRef)
        }
        DataType::Int32 => {
            let arr = Int32Array::from(
                normalized_values
                    .iter()
                    .map(|v| v.and_then(|s| s.parse::<i32>().ok()))
                    .collect::<Vec<_>>(),
            );
            Ok(Arc::new(arr) as ArrayRef)
        }
        DataType::Int64 => {
            let arr = Int64Array::from(
                normalized_values
                    .iter()
                    .map(|v| v.and_then(|s| s.parse::<i64>().ok()))
                    .collect::<Vec<_>>(),
            );
            Ok(Arc::new(arr) as ArrayRef)
        }
        DataType::Float32 => {
            let arr = Float32Array::from(
                normalized_values
                    .iter()
                    .map(|v| v.and_then(|s| s.parse::<f32>().ok()))
                    .collect::<Vec<_>>(),
            );
            Ok(Arc::new(arr) as ArrayRef)
        }
        DataType::Float64 => {
            let arr = Float64Array::from(
                normalized_values
                    .iter()
                    .map(|v| v.and_then(|s| s.parse::<f64>().ok()))
                    .collect::<Vec<_>>(),
            );
            Ok(Arc::new(arr) as ArrayRef)
        }
        DataType::Boolean => {
            let arr = BooleanArray::from(
                normalized_values
                    .iter()
                    .map(|v| v.and_then(|s| s.parse::<bool>().ok()))
                    .collect::<Vec<_>>(),
            );
            Ok(Arc::new(arr) as ArrayRef)
        }
        DataType::Date32 => {
            let arr = Date32Array::from(
                normalized_values
                    .iter()
                    .map(|v| v.and_then(parse_date_to_days))
                    .collect::<Vec<_>>(),
            );
            Ok(Arc::new(arr) as ArrayRef)
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let arr = TimestampMicrosecondArray::from(
                normalized_values
                    .iter()
                    .map(|v| v.and_then(parse_timestamp_micros))
                    .collect::<Vec<_>>(),
            );
            Ok(Arc::new(arr) as ArrayRef)
        }
        _ => {
            let arr = StringArray::from(normalized_values);
            Ok(Arc::new(arr) as ArrayRef)
        }
    }
}

fn normalize_partition_value(value: Option<&str>) -> Option<&str> {
    match value {
        Some("__HIVE_DEFAULT_PARTITION__") | Some("") => None,
        _ => value,
    }
}

/// Parse a date string like "2024-01-15" into days since epoch (Date32).
fn parse_date_to_days(s: &str) -> Option<i32> {
    let parts: Vec<&str> = s.splitn(3, '-').collect();
    if parts.len() != 3 {
        return None;
    }
    let year = parts[0].parse::<i32>().ok()?;
    let month = parts[1].parse::<u32>().ok()?;
    let day = parts[2].parse::<u32>().ok()?;
    days_since_epoch(year, month, day)
}

/// Compute days since 1970-01-01 using the proleptic Gregorian calendar.
fn days_since_epoch(year: i32, month: u32, day: u32) -> Option<i32> {
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    let days_in_months = [0u32, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    let leap = is_leap_year(year);
    let feb_days = if leap { 29u32 } else { 28u32 };

    let mut day_of_year: i64 = 0;
    for m in 1..month {
        day_of_year += if m == 2 {
            feb_days
        } else {
            days_in_months[m as usize]
        } as i64;
    }
    day_of_year += day as i64 - 1;

    let epoch_year = 1970i32;
    let mut total_days: i64 = 0;
    if year >= epoch_year {
        for y in epoch_year..year {
            total_days += if is_leap_year(y) { 366 } else { 365 };
        }
    } else {
        for y in year..epoch_year {
            total_days -= if is_leap_year(y) { 366 } else { 365 };
        }
    }
    total_days += day_of_year;
    Some(total_days as i32)
}

fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)
}

/// Parse a timestamp string like "2024-01-15 12:34:56" or "2024-01-15T12:34:56"
/// into microseconds since epoch.
fn parse_timestamp_micros(s: &str) -> Option<i64> {
    let s = s.replace('T', " ");
    let parts: Vec<&str> = s.splitn(2, ' ').collect();
    if parts.len() != 2 {
        return None;
    }
    let days = parse_date_to_days(parts[0])? as i64;
    let time_parts: Vec<&str> = parts[1].splitn(3, ':').collect();
    if time_parts.len() < 2 {
        return None;
    }
    let hour = time_parts[0].parse::<i64>().ok()?;
    let minute = time_parts[1].parse::<i64>().ok()?;
    let seconds_str = if time_parts.len() == 3 {
        time_parts[2]
    } else {
        "0"
    };
    let sec_parts: Vec<&str> = seconds_str.splitn(2, '.').collect();
    let seconds = sec_parts[0].parse::<i64>().ok()?;
    let micros_frac = if sec_parts.len() == 2 {
        let frac = sec_parts[1];
        let padded = format!("{:0<6}", frac);
        padded[..6].parse::<i64>().ok()?
    } else {
        0
    };

    let micros = days * 86_400_000_000
        + hour * 3_600_000_000
        + minute * 60_000_000
        + seconds * 1_000_000
        + micros_frac;
    Some(micros)
}

fn parse_partition_value(s: &str, data_type: &DataType) -> Result<ScalarValue> {
    use datafusion::scalar::ScalarValue;

    if s == "__HIVE_DEFAULT_PARTITION__" || s.is_empty() {
        return ScalarValue::try_from(data_type);
    }

    match data_type {
        DataType::Int8 => s
            .parse::<i8>()
            .map(|v| ScalarValue::Int8(Some(v)))
            .map_err(|e| DataFusionError::External(Box::new(e))),
        DataType::Int16 => s
            .parse::<i16>()
            .map(|v| ScalarValue::Int16(Some(v)))
            .map_err(|e| DataFusionError::External(Box::new(e))),
        DataType::Int32 => s
            .parse::<i32>()
            .map(|v| ScalarValue::Int32(Some(v)))
            .map_err(|e| DataFusionError::External(Box::new(e))),
        DataType::Int64 => s
            .parse::<i64>()
            .map(|v| ScalarValue::Int64(Some(v)))
            .map_err(|e| DataFusionError::External(Box::new(e))),
        DataType::Float32 => s
            .parse::<f32>()
            .map(|v| ScalarValue::Float32(Some(v)))
            .map_err(|e| DataFusionError::External(Box::new(e))),
        DataType::Float64 => s
            .parse::<f64>()
            .map(|v| ScalarValue::Float64(Some(v)))
            .map_err(|e| DataFusionError::External(Box::new(e))),
        DataType::Boolean => s
            .parse::<bool>()
            .map(|v| ScalarValue::Boolean(Some(v)))
            .map_err(|e| DataFusionError::External(Box::new(e))),
        DataType::Utf8 => Ok(ScalarValue::Utf8(Some(s.to_string()))),
        DataType::Date32 => parse_date_to_days(s)
            .map(|v| ScalarValue::Date32(Some(v)))
            .ok_or_else(|| DataFusionError::Internal(format!("failed to parse date: {}", s))),
        DataType::Timestamp(TimeUnit::Microsecond, tz) => parse_timestamp_micros(s)
            .map(|v| ScalarValue::TimestampMicrosecond(Some(v), tz.clone()))
            .ok_or_else(|| DataFusionError::Internal(format!("failed to parse timestamp: {}", s))),
        _ => Ok(ScalarValue::Utf8(Some(s.to_string()))),
    }
}

async fn list_files(
    object_store: &Arc<dyn ObjectStore>,
    directory_full_location: &str,
) -> Result<Vec<ObjectMeta>> {
    let relative_path = location_to_object_store_path(directory_full_location)?;

    let file_object_metas: Vec<_> = object_store
        .list(Some(&relative_path))
        .collect::<Vec<_>>()
        .await;

    let mut results = Vec::new();
    for file_object_meta in file_object_metas {
        let meta = file_object_meta.map_err(|e| DataFusionError::External(Box::new(e)))?;
        let file_name = meta.location.filename().unwrap_or("");

        // ignore empty object
        if meta.size == 0 {
            continue;
        }

        // ignore hidden file
        if file_name.starts_with('_') || file_name.starts_with('.') {
            continue;
        }
        results.push(meta);
    }
    Ok(results)
}

async fn collect_partitioned_files<I, Fut>(
    tasks: I,
    concurrency: usize,
) -> Result<Vec<PartitionedFile>>
where
    I: IntoIterator<Item = Fut>,
    Fut: Future<Output = Result<Vec<PartitionedFile>>>,
{
    let partitioned_files = stream::iter(tasks)
        .buffer_unordered(concurrency)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?;

    Ok(partitioned_files.into_iter().flatten().collect())
}

fn location_to_object_store_path(location: &str) -> Result<Path> {
    let parsed = Url::parse(location).map_err(|e| DataFusionError::External(e.into()))?;
    Ok(Path::from(parsed.path().trim_start_matches('/')))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::Field;
    use datafusion::logical_expr::expr::InList;
    use datafusion::logical_expr::{Expr, Operator, binary_expr, col, lit};
    use datafusion::object_store::memory::InMemory;
    use datafusion::prelude::SessionContext;
    use std::collections::HashMap;
    use std::pin::Pin;
    use tokio::time::{Duration, sleep};

    #[test]
    fn test_location_to_object_store_path() {
        let path = location_to_object_store_path(
            "s3://warehouse/hive/tpch_hive.db/textfile_no_partition_table",
        )
        .unwrap();
        assert_eq!(
            path.as_ref(),
            "hive/tpch_hive.db/textfile_no_partition_table"
        );
    }

    #[test]
    fn test_location_to_object_store_path_with_partition() {
        let path = location_to_object_store_path(
            "s3://warehouse/hive/tpch_hive.db/textfile_partition_table/p=1",
        )
        .unwrap();
        assert_eq!(
            path.as_ref(),
            "hive/tpch_hive.db/textfile_partition_table/p=1"
        );
    }

    #[test]
    fn test_prune_partitions_eq() {
        let state = SessionContext::new();
        let partition_fields = partition_fields();
        let partitions = sample_partitions();

        let surviving = prune_partitions(
            &partitions,
            &partition_fields,
            &[binary_expr(col("dt"), Operator::Eq, lit("2012-01-03"))],
            &state.state(),
        )
        .unwrap();

        assert_eq!(surviving, vec![1, 5]);
    }

    #[test]
    fn test_prune_partitions_in_list_and_gt() {
        let state = SessionContext::new();
        let partition_fields = partition_fields();
        let partitions = sample_partitions();

        let in_list = Expr::InList(InList::new(
            Box::new(col("dt")),
            vec![lit("2012-01-01"), lit("2012-01-04")],
            false,
        ));
        let gt = binary_expr(col("dt"), Operator::Gt, lit("2012-01-01"));

        let surviving = prune_partitions(
            &partitions,
            &partition_fields,
            &[in_list, gt],
            &state.state(),
        )
        .unwrap();

        assert_eq!(surviving, vec![2]);
    }

    #[test]
    fn test_prune_partitions_eq_on_multiple_partition_columns() {
        let state = SessionContext::new();
        let partition_fields = partition_fields();
        let partitions = sample_partitions();

        let surviving = prune_partitions(
            &partitions,
            &partition_fields,
            &[
                binary_expr(col("dt"), Operator::Eq, lit("2012-01-03")),
                binary_expr(col("bucket"), Operator::Eq, lit(2_i32)),
            ],
            &state.state(),
        )
        .unwrap();

        assert_eq!(surviving, vec![1]);
    }

    #[test]
    fn test_prune_partitions_ignores_mixed_data_filters() {
        let state = SessionContext::new();
        let partition_fields = partition_fields();
        let partitions = sample_partitions();

        let surviving = prune_partitions(
            &partitions,
            &partition_fields,
            &[
                binary_expr(col("dt"), Operator::Eq, lit("2012-01-03")),
                binary_expr(col("c1"), Operator::Gt, lit(10_i32)),
            ],
            &state.state(),
        )
        .unwrap();

        assert_eq!(surviving, vec![1, 5]);
    }

    #[test]
    fn test_prune_partitions_without_partition_filter_keeps_all() {
        let state = SessionContext::new();
        let partition_fields = partition_fields();
        let partitions = sample_partitions();

        let surviving = prune_partitions(
            &partitions,
            &partition_fields,
            &[binary_expr(col("c1"), Operator::Gt, lit(10_i32))],
            &state.state(),
        )
        .unwrap();

        assert_eq!(surviving, vec![0, 1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_prune_partitions_null_partition_does_not_match() {
        let state = SessionContext::new();
        let partition_fields = partition_fields();
        let partitions = sample_partitions();

        let surviving = prune_partitions(
            &partitions,
            &partition_fields,
            &[binary_expr(col("dt"), Operator::Eq, lit("2012-01-03"))],
            &state.state(),
        )
        .unwrap();

        assert_eq!(surviving, vec![1, 5]);
    }
    #[test]
    fn test_prune_partitions_is_null_matches_null_partitions() {
        let state = SessionContext::new();
        let partition_fields = partition_fields();
        let partitions = sample_partitions();

        let surviving = prune_partitions(
            &partitions,
            &partition_fields,
            &[Expr::IsNull(Box::new(col("dt")))],
            &state.state(),
        )
        .unwrap();

        assert_eq!(surviving, vec![3, 4]);
    }

    #[test]
    fn test_prune_partitions_bucket_is_not_null_matches_non_null_partitions() {
        let state = SessionContext::new();
        let partition_fields = partition_fields();
        let partitions = sample_partitions();

        let surviving = prune_partitions(
            &partitions,
            &partition_fields,
            &[Expr::IsNotNull(Box::new(col("bucket")))],
            &state.state(),
        )
        .unwrap();

        assert_eq!(surviving, vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn test_prune_partitions_dt_eq_and_bucket_is_null_matches_null_bucket_partition() {
        let state = SessionContext::new();
        let partition_fields = partition_fields();
        let partitions = sample_partitions();

        let surviving = prune_partitions(
            &partitions,
            &partition_fields,
            &[
                binary_expr(col("dt"), Operator::Eq, lit("2012-01-03")),
                Expr::IsNull(Box::new(col("bucket"))),
            ],
            &state.state(),
        )
        .unwrap();

        assert_eq!(surviving, vec![5]);
    }

    #[test]

    fn test_prune_partitions_dt_is_null_and_bucket_eq_matches_intersection() {
        let state = SessionContext::new();
        let partition_fields = partition_fields();
        let partitions = sample_partitions();

        let surviving = prune_partitions(
            &partitions,
            &partition_fields,
            &[
                Expr::IsNull(Box::new(col("dt"))),
                binary_expr(col("bucket"), Operator::Eq, lit(2_i32)),
            ],
            &state.state(),
        )
        .unwrap();

        assert_eq!(surviving, vec![3]);
    }

    #[test]
    fn test_prune_partitions_is_null_and_eq_matches_nothing() {
        let state = SessionContext::new();
        let partition_fields = partition_fields();
        let partitions = sample_partitions();

        let surviving = prune_partitions(
            &partitions,
            &partition_fields,
            &[
                Expr::IsNull(Box::new(col("dt"))),
                binary_expr(col("dt"), Operator::Eq, lit("2012-01-03")),
            ],
            &state.state(),
        )
        .unwrap();

        assert_eq!(surviving, Vec::<usize>::new());
    }

    fn partition_fields() -> Vec<Arc<Field>> {
        vec![
            Arc::new(Field::new("dt", DataType::Utf8, true)),
            Arc::new(Field::new("bucket", DataType::Int32, true)),
        ]
    }

    fn sample_partitions() -> Vec<HivePartition> {
        vec![
            HivePartition {
                location: "s3://warehouse/hive/tpch_hive.db/parquet_table/dt=2012-01-01/bucket=1"
                    .to_string(),
                partition_values: vec!["2012-01-01".to_string(), "1".to_string()],
            },
            HivePartition {
                location: "s3://warehouse/hive/tpch_hive.db/parquet_table/dt=2012-01-03/bucket=2"
                    .to_string(),
                partition_values: vec!["2012-01-03".to_string(), "2".to_string()],
            },
            HivePartition {
                location: "s3://warehouse/hive/tpch_hive.db/parquet_table/dt=2012-01-04/bucket=3"
                    .to_string(),
                partition_values: vec!["2012-01-04".to_string(), "3".to_string()],
            },
            HivePartition {
                location:
                    "s3://warehouse/hive/tpch_hive.db/parquet_table/dt=__HIVE_DEFAULT_PARTITION__/bucket=2"
                        .to_string(),
                partition_values: vec!["__HIVE_DEFAULT_PARTITION__".to_string(), "2".to_string()],
            },
            HivePartition {
                location: "s3://warehouse/hive/tpch_hive.db/parquet_table/dt=/bucket=1"
                    .to_string(),
                partition_values: vec!["".to_string(), "1".to_string()],
            },
            HivePartition {
                location:
                    "s3://warehouse/hive/tpch_hive.db/parquet_table/dt=2012-01-03/bucket=__HIVE_DEFAULT_PARTITION__"
                    .to_string(),
                partition_values: vec![
                    "2012-01-03".to_string(),
                    "__HIVE_DEFAULT_PARTITION__".to_string(),
                ],
            },
        ]
    }

    async fn put_test_object(store: &Arc<dyn ObjectStore>, path: &str, data: &[u8]) {
        store
            .put(&Path::from(path), data.to_vec().into())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_collect_partitioned_files_preserves_partition_values() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        put_test_object(&store, "table/dt=2024-01-01/file1.parquet", b"a").await;
        put_test_object(&store, "table/dt=2024-01-02/file2.parquet", b"b").await;

        let tasks: Vec<Pin<Box<dyn Future<Output = Result<Vec<PartitionedFile>>>>>> = vec![
            {
                let store = Arc::clone(&store);
                Box::pin(async move {
                    sleep(Duration::from_millis(30)).await;
                    let partition_values = vec![ScalarValue::Utf8(Some("2024-01-01".to_string()))];
                    let files = list_files(&store, "memory:///table/dt=2024-01-01")
                        .await?
                        .into_iter()
                        .map(|file_object_meta| {
                            let mut partitioned_file = PartitionedFile::from(file_object_meta);
                            partitioned_file.partition_values = partition_values.clone();
                            partitioned_file
                        })
                        .collect();
                    Ok(files)
                })
            },
            {
                let store = Arc::clone(&store);
                Box::pin(async move {
                    let partition_values = vec![ScalarValue::Utf8(Some("2024-01-02".to_string()))];
                    let files = list_files(&store, "memory:///table/dt=2024-01-02")
                        .await?
                        .into_iter()
                        .map(|file_object_meta| {
                            let mut partitioned_file = PartitionedFile::from(file_object_meta);
                            partitioned_file.partition_values = partition_values.clone();
                            partitioned_file
                        })
                        .collect();
                    Ok(files)
                })
            },
        ];

        let files = collect_partitioned_files(tasks, 2).await.unwrap();

        assert_eq!(files.len(), 2);
        let file_to_partition_values: HashMap<&str, Vec<ScalarValue>> = files
            .iter()
            .map(|file| {
                (
                    file.object_meta.location.as_ref(),
                    file.partition_values.clone(),
                )
            })
            .collect();

        assert_eq!(
            file_to_partition_values.get("table/dt=2024-01-01/file1.parquet"),
            Some(&vec![ScalarValue::Utf8(Some("2024-01-01".to_string()))])
        );
        assert_eq!(
            file_to_partition_values.get("table/dt=2024-01-02/file2.parquet"),
            Some(&vec![ScalarValue::Utf8(Some("2024-01-02".to_string()))])
        );
    }

    #[tokio::test]
    async fn test_collect_partitioned_files_fail_fast_on_partition_error() {
        let tasks: Vec<Pin<Box<dyn Future<Output = Result<Vec<PartitionedFile>>>>>> = vec![
            Box::pin(async {
                sleep(Duration::from_millis(20)).await;
                Ok(Vec::<PartitionedFile>::new())
            }),
            Box::pin(async {
                Err(DataFusionError::Internal(
                    "failed to list one partition".to_string(),
                ))
            }),
        ];

        let err = collect_partitioned_files(tasks, 2).await.unwrap_err();

        assert!(err.to_string().contains("failed to list one partition"));
    }

    #[tokio::test]
    async fn test_list_files_filters_hidden_and_empty_objects() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        put_test_object(&store, "table/dt=2024-01-01/file1.parquet", b"a").await;
        put_test_object(&store, "table/dt=2024-01-01/_temporary", b"a").await;
        put_test_object(&store, "table/dt=2024-01-01/.metadata", b"a").await;
        put_test_object(&store, "table/dt=2024-01-01/empty.parquet", b"").await;

        let files = list_files(&store, "memory:///table/dt=2024-01-01")
            .await
            .unwrap();

        assert_eq!(files.len(), 1);
        assert_eq!(
            files[0].location.as_ref(),
            "table/dt=2024-01-01/file1.parquet"
        );
    }
}
