use crate::storage::StorageCredential;
use crate::table_format::hive::hive_partition::HivePartition;
use crate::table_format::hive::hive_storage_info::HiveInputFormat;
use crate::table_format::hive::HiveStorageInfo;
use async_trait::async_trait;
use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, Int8Array, StringArray, TimestampMicrosecondArray,
};
use datafusion::arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use datafusion::catalog::memory::DataSourceExec;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::config::CsvOptions;
use datafusion::common::parsers::CompressionTypeVariant;
use datafusion::common::{Result, ToDFSchema};
use datafusion::config::TableParquetOptions;
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{
    CsvSource, FileGroup, FileScanConfigBuilder, ParquetSource,
};
use datafusion::datasource::table_schema::TableSchema;
use datafusion::datasource::TableType;
use datafusion::error::DataFusionError;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::utils::conjunction;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::object_store::path::Path;
use datafusion::object_store::{ObjectMeta, ObjectStore};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::scalar::ScalarValue;
use futures::StreamExt;
use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

#[derive(Debug)]
pub struct HiveTableProvider {
    hive_storage_info: HiveStorageInfo,
    partitions: Vec<HivePartition>,
    storage_credential: Option<StorageCredential>,
}

impl HiveTableProvider {
    pub fn new(
        hive_storage_info: HiveStorageInfo,
        partitions: Vec<HivePartition>,
        storage_credential: Option<StorageCredential>,
    ) -> Self {
        Self {
            hive_storage_info,
            partitions,
            storage_credential,
        }
    }
}

// impl HiveTableProvider {
//     pub fn try_new(info: HiveStorageInfo, hms_partitions: Vec<Partition>) -> Result<Self> {
//         let partition_col_name_set: HashSet<String> = info
//             .partition_cols
//             .iter()
//             .map(|(name, _)| name.to_ascii_lowercase())
//             .collect();
//
//         let data_fields: Vec<Field> = info
//             .data_cols
//             .iter()
//             .filter(|(name, _)| !partition_col_name_set.contains(&name.to_ascii_lowercase()))
//             .map(|(name, hive_type)| {
//                 let dt = hive_type_to_arrow(hive_type)?;
//                 Ok(Field::new(name, dt, true))
//             })
//             .collect::<Result<Vec<_>>>()?;
//
//         let partition_fields: Vec<Field> = info
//             .partition_cols
//             .iter()
//             .map(|(name, hive_type)| {
//                 let dt = hive_type_to_arrow(hive_type)?;
//                 Ok(Field::new(name, dt, true))
//             })
//             .collect::<Result<Vec<_>>>()?;
//
//         let data_schema = Arc::new(Schema::new(data_fields));
//
//         let mut all_fields = data_schema.fields().to_vec();
//         all_fields.extend(partition_fields.iter().map(|f| Arc::new(f.clone())));
//         let full_schema = Arc::new(Schema::new(all_fields));
//
//         let partition_col_fields: Vec<FieldRef> =
//             partition_fields.iter().map(|f| Arc::new(f.clone())).collect();
//
//         let partitions = hms_partitions
//             .into_iter()
//             .filter_map(|p| {
//                 let location = p.sd?.location?.to_string();
//                 let values = p.values?.iter().map(|v| v.to_string()).collect();
//                 Some(HivePartitionInfo { location, values })
//             })
//             .collect();
//
//         Ok(Self {
//             schema: full_schema,
//             data_schema,
//             input_format: info.input_format,
//             location: info.location,
//             partitions,
//             partition_col_fields,
//             serde_properties: info.serde_properties,
//             storage_credential: info.storage_credential,
//         })
//     }
// }

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
        if let Some(cred) = &self.storage_credential {
            cred.register_into_session(&self.hive_storage_info.table_location, state)?;
        }

        let store_url = ObjectStoreUrl::parse(&self.hive_storage_info.table_location)?;

        let object_store = state.runtime_env().object_store(&store_url)?;

        let scan_file_list: Vec<PartitionedFile> = if self.partitions.is_empty() {
            let file_object_metas = list_files(
                &object_store,
                &self.hive_storage_info.table_location,
                &self.hive_storage_info.table_location,
            )
            .await?;
            file_object_metas
                .iter()
                .map(|file_object_meta| PartitionedFile::from(file_object_meta.clone()))
                .collect()
        } else {
            let mut result: Vec<PartitionedFile> = Vec::new();
            for partition in &self.partitions {
                let file_object_metas = list_files(
                    &object_store,
                    &self.hive_storage_info.table_location,
                    &partition.location,
                )
                .await?;
                let partition_values: Vec<ScalarValue> = self
                    .hive_storage_info
                    .table_schema
                    .table_partition_cols()
                    .iter()
                    .zip(partition.partition_values.iter())
                    .map(|(field, val)| parse_partition_value(val, field.data_type()))
                    .collect::<Result<Vec<_>>>()?;
                result.extend(file_object_metas.iter().map(|file_object_meta| {
                    let mut partition_field = PartitionedFile::from(file_object_meta.clone());
                    partition_field.partition_values = partition_values.clone();
                    partition_field
                }));
            }
            result
        };

        // let surviving_partition_indices = if self.partitions.is_empty() {
        //     vec![]
        // } else {
        //     prune_partitions(
        //         &self.partitions,
        //         &self.partition_col_fields,
        //         filters,
        //         state,
        //     )?
        // };
        //
        // let (path_schema, path_host) = parse_location_schema_host(&self.location)?;
        // let store_url = ObjectStoreUrl::parse(format!("{}://{}", path_schema, path_host))?;
        // let object_store = state.runtime_env().object_store(&store_url)?;
        //
        // let mut all_files: Vec<PartitionedFile> = Vec::new();

        // if self.partitions.is_empty() {
        //     let files = list_files(
        //         &object_store,
        //         &self.location,
        //         &path_schema,
        //         &path_host,
        //     )
        //     .await?;
        //     all_files.extend(files.into_iter().map(|(path, size)| {
        //         PartitionedFile::new(path, size)
        //     }));
        // } else {
        //     for idx in surviving_partition_indices {
        //         let partition = &self.partitions[idx];
        //         let files = list_files(
        //             &object_store,
        //             &partition.location,
        //             &path_schema,
        //             &path_host,
        //         )
        //         .await?;
        //
        //         let partition_values: Vec<datafusion::scalar::ScalarValue> = self
        //             .partition_col_fields
        //             .iter()
        //             .zip(partition.values.iter())
        //             .map(|(field, val)| parse_partition_value(val, field.data_type()))
        //             .collect::<Result<Vec<_>>>()?;
        //
        //         for (path, size) in files {
        //             let mut pf = PartitionedFile::new(path, size);
        //             pf.partition_values = partition_values.clone();
        //             all_files.push(pf);
        //         }
        //     }
        // }
        //
        // let table_schema = TableSchema::new(
        //     self.data_schema.clone(),
        //     self.partition_col_fields.clone(),
        // );

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
                &filter_data_filters(filters, &self.hive_storage_info.table_schema.file_schema()),
                state,
                &self.hive_storage_info.table_schema.file_schema(),
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

// fn prune_partitions(
//     partitions: &[HivePartitionInfo],
//     partition_fields: &[FieldRef],
//     filters: &[Expr],
//     state: &dyn Session,
// ) -> Result<Vec<usize>> {
//     if filters.is_empty() || partition_fields.is_empty() {
//         return Ok((0..partitions.len()).collect());
//     }
//
//     let partition_schema = Arc::new(Schema::new(
//         partition_fields
//             .iter()
//             .map(|f| f.as_ref().clone())
//             .collect::<Vec<_>>(),
//     ));
//
//     let partition_filters: Vec<&Expr> = filters
//         .iter()
//         .filter(|f| {
//             f.column_refs()
//                 .iter()
//                 .all(|c| partition_schema.field_with_name(c.name()).is_ok())
//         })
//         .collect();
//
//     if partition_filters.is_empty() {
//         return Ok((0..partitions.len()).collect());
//     }
//
//     let df_schema = partition_schema.as_ref().clone().to_dfschema()?;
//
//     let mut columns: Vec<ArrayRef> = Vec::new();
//     for (i, field) in partition_fields.iter().enumerate() {
//         let values: Vec<Option<&str>> = partitions
//             .iter()
//             .map(|p| p.values.get(i).map(|s| s.as_str()))
//             .collect();
//         let array = build_partition_array(field.data_type(), &values)?;
//         columns.push(array);
//     }
//
//     let batch = datafusion::arrow::record_batch::RecordBatch::try_new(
//         partition_schema.clone(),
//         columns,
//     )
//     .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
//
//     let mut surviving = Vec::new();
//
//     for row_idx in 0..partitions.len() {
//         let mut keep = true;
//         for filter_expr in &partition_filters {
//             let physical_expr =
//                 state.create_physical_expr((*filter_expr).clone(), &df_schema)?;
//             let result = physical_expr.evaluate(&batch)?;
//             let result_array = result.into_array(batch.num_rows())?;
//             let bool_array = result_array
//                 .as_any()
//                 .downcast_ref::<BooleanArray>()
//                 .ok_or_else(|| {
//                     DataFusionError::Internal(
//                         "partition filter did not produce boolean array".into(),
//                     )
//                 })?;
//             if bool_array.is_null(row_idx) || !bool_array.value(row_idx) {
//                 keep = false;
//                 break;
//             }
//         }
//         if keep {
//             surviving.push(row_idx);
//         }
//     }
//
//     Ok(surviving)
// }

fn build_partition_array(data_type: &DataType, values: &[Option<&str>]) -> Result<ArrayRef> {
    match data_type {
        DataType::Int8 => {
            let arr = Int8Array::from(
                values
                    .iter()
                    .map(|v| v.and_then(|s| s.parse::<i8>().ok()))
                    .collect::<Vec<_>>(),
            );
            Ok(Arc::new(arr) as ArrayRef)
        }
        DataType::Int16 => {
            let arr = Int16Array::from(
                values
                    .iter()
                    .map(|v| v.and_then(|s| s.parse::<i16>().ok()))
                    .collect::<Vec<_>>(),
            );
            Ok(Arc::new(arr) as ArrayRef)
        }
        DataType::Int32 => {
            let arr = Int32Array::from(
                values
                    .iter()
                    .map(|v| v.and_then(|s| s.parse::<i32>().ok()))
                    .collect::<Vec<_>>(),
            );
            Ok(Arc::new(arr) as ArrayRef)
        }
        DataType::Int64 => {
            let arr = Int64Array::from(
                values
                    .iter()
                    .map(|v| v.and_then(|s| s.parse::<i64>().ok()))
                    .collect::<Vec<_>>(),
            );
            Ok(Arc::new(arr) as ArrayRef)
        }
        DataType::Float32 => {
            let arr = Float32Array::from(
                values
                    .iter()
                    .map(|v| v.and_then(|s| s.parse::<f32>().ok()))
                    .collect::<Vec<_>>(),
            );
            Ok(Arc::new(arr) as ArrayRef)
        }
        DataType::Float64 => {
            let arr = Float64Array::from(
                values
                    .iter()
                    .map(|v| v.and_then(|s| s.parse::<f64>().ok()))
                    .collect::<Vec<_>>(),
            );
            Ok(Arc::new(arr) as ArrayRef)
        }
        DataType::Boolean => {
            let arr = BooleanArray::from(
                values
                    .iter()
                    .map(|v| v.and_then(|s| s.parse::<bool>().ok()))
                    .collect::<Vec<_>>(),
            );
            Ok(Arc::new(arr) as ArrayRef)
        }
        DataType::Date32 => {
            let arr = Date32Array::from(
                values
                    .iter()
                    .map(|v| v.and_then(|s| parse_date_to_days(s)))
                    .collect::<Vec<_>>(),
            );
            Ok(Arc::new(arr) as ArrayRef)
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let arr = TimestampMicrosecondArray::from(
                values
                    .iter()
                    .map(|v| v.and_then(|s| parse_timestamp_micros(s)))
                    .collect::<Vec<_>>(),
            );
            Ok(Arc::new(arr) as ArrayRef)
        }
        _ => {
            let arr = StringArray::from(values.to_vec());
            Ok(Arc::new(arr) as ArrayRef)
        }
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
    if month < 1 || month > 12 || day < 1 || day > 31 {
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
    table_location: &str,
    full_location: &str,
) -> Result<Vec<ObjectMeta>> {
    let relative_path = full_location
        .strip_prefix(table_location)
        .ok_or_else(|| DataFusionError::Internal(String::from("extract path failed")))?;
    let relative_path = Path::from(relative_path);

    let file_object_metas: Vec<_> = object_store
        .list(Some(&relative_path))
        .collect::<Vec<_>>()
        .await;

    let mut results = Vec::new();
    for file_object_meta in file_object_metas {
        let meta = file_object_meta.map_err(|e| DataFusionError::External(Box::new(e)))?;
        let file_name = meta.location.filename().unwrap_or("");

        if file_name.starts_with('_') || file_name.starts_with('.') {
            continue;
        }
        results.push(meta);
    }
    Ok(results)
}
