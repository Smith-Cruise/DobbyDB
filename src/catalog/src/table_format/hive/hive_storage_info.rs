use crate::table_format::hive::hive_type::hive_type_to_arrow_type;
use aws_sdk_glue::types::{Column as GlueColumn, Table as GlueTable};
use datafusion::common::{DataFusionError, Result};
use datafusion::datasource::table_schema::TableSchema;
use deltalake::arrow::datatypes::{Field, Schema};
use hive_metastore::{FieldSchema, Table as HMSTable};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug)]
pub enum HiveInputFormat {
    TextFile,
    Parquet,
    ORC,
}

#[derive(Debug)]
pub struct HiveStorageInfo {
    pub table_location: String,
    pub input_format: HiveInputFormat,
    pub table_schema: TableSchema,
    pub serde_properties: HashMap<String, String>,
}

impl HiveStorageInfo {
    pub fn try_new_from_hms_table(table: &HMSTable) -> Result<Self> {
        let sd = table.sd.as_ref().ok_or_else(|| {
            DataFusionError::Internal("Storage descriptor not existed".to_string())
        })?;
        let data_cols = Self::extract_hms_field_schemas(&sd.cols)?;
        let table_partition_cols = Self::extract_hms_field_schemas(&table.partition_keys)?;
        let serde_properties = sd
            .serde_info
            .as_ref()
            .and_then(|s| s.parameters.as_ref())
            .map(|p| {
                p.iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect()
            })
            .unwrap_or_default();

        Self::try_new(
            sd.location.as_deref(),
            sd.input_format.as_deref(),
            data_cols,
            table_partition_cols,
            serde_properties,
        )
    }

    pub fn try_new_from_glue_table(table: &GlueTable) -> Result<Self> {
        let sd = table.storage_descriptor.as_ref().ok_or_else(|| {
            DataFusionError::Internal("Storage descriptor not existed".to_string())
        })?;
        let data_cols = Self::extract_glue_field_schemas(sd.columns())?;
        let table_partition_cols = Self::extract_glue_field_schemas(table.partition_keys())?;
        let serde_properties = sd
            .serde_info()
            .and_then(|s| s.parameters())
            .cloned()
            .unwrap_or_default();

        Self::try_new(
            sd.location(),
            sd.input_format(),
            data_cols,
            table_partition_cols,
            serde_properties,
        )
    }

    pub fn try_get_input_format(input_format: &str) -> Result<HiveInputFormat> {
        if input_format.to_lowercase().contains("text") {
            Ok(HiveInputFormat::TextFile)
        } else if input_format.to_lowercase().contains("parquet") {
            Ok(HiveInputFormat::Parquet)
        } else if input_format.to_lowercase().contains("orc") {
            Ok(HiveInputFormat::ORC)
        } else {
            Err(DataFusionError::NotImplemented(format!(
                "unsupported Hive input format: {}",
                input_format
            )))
        }
    }

    fn try_new(
        table_location: Option<&str>,
        input_format: Option<&str>,
        data_cols: Vec<Arc<Field>>,
        table_partition_cols: Vec<Arc<Field>>,
        serde_properties: HashMap<String, String>,
    ) -> Result<Self> {
        let table_location = table_location
            .map(ToString::to_string)
            .ok_or_else(|| DataFusionError::Internal("location not exist".to_string()))?;
        let input_format = match input_format {
            Some(input_format) => Self::try_get_input_format(input_format)?,
            None => {
                return Err(DataFusionError::Internal(
                    "input format not existed".to_string(),
                ));
            }
        };

        Ok(Self {
            table_location,
            input_format,
            table_schema: TableSchema::new(Arc::new(Schema::new(data_cols)), table_partition_cols),
            serde_properties,
        })
    }

    fn extract_hms_field_schemas(
        field_schemas: &Option<Vec<FieldSchema>>,
    ) -> Result<Vec<Arc<Field>>> {
        let fields: Result<Vec<(String, String)>> = field_schemas
            .as_ref()
            .map(|field_schemas| {
                field_schemas
                    .iter()
                    .map(|field_schema| {
                        let name = field_schema.name.as_ref().ok_or_else(|| {
                            DataFusionError::Internal("FieldSchema's name not existed".to_string())
                        })?;
                        let ty = field_schema.r#type.as_ref().ok_or_else(|| {
                            DataFusionError::Internal("FieldSchema's type not existed".to_string())
                        })?;
                        Ok((name.to_string(), ty.to_string()))
                    })
                    .collect()
            })
            .unwrap_or_else(|| Ok(Vec::new()));

        Self::extract_arrow_fields(fields?)
    }

    fn extract_glue_field_schemas(field_schemas: &[GlueColumn]) -> Result<Vec<Arc<Field>>> {
        let fields: Result<Vec<(String, String)>> = field_schemas
            .iter()
            .map(|field_schema| {
                let ty = field_schema.r#type().ok_or_else(|| {
                    DataFusionError::Internal("FieldSchema's type not existed".to_string())
                })?;
                Ok((field_schema.name().to_string(), ty.to_string()))
            })
            .collect();

        Self::extract_arrow_fields(fields?)
    }

    fn extract_arrow_fields(fields: Vec<(String, String)>) -> Result<Vec<Arc<Field>>> {
        let fields: Result<Vec<Arc<Field>>> = fields
            .iter()
            .map(|(name, ty)| {
                Ok(Arc::new(Field::new(
                    name,
                    hive_type_to_arrow_type(ty)?,
                    true,
                )))
            })
            .collect();
        fields
    }
}
