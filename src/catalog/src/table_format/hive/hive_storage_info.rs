use crate::table_format::hive::hive_type::hive_type_to_arrow_type;
use datafusion::common::{DataFusionError, Result};
use datafusion::datasource::table_schema::TableSchema;
use deltalake::arrow::datatypes::{Field, Schema};
use hive_metastore::{FieldSchema, Table};
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
    pub fn try_new_from_hms_table(table: &Table) -> Result<Self> {
        let sd = if let Some(sd) = &table.sd {
            sd
        } else {
            return Err(DataFusionError::Internal(String::from(
                "Storage descriptor not existed",
            )));
        };
        let table_location = match &sd.location {
            Some(l) => l.to_string(),
            None => {
                return Err(DataFusionError::Internal(String::from(
                    "location not exist",
                )));
            }
        };
        let input_format = match &sd.input_format {
            Some(str) => Self::try_get_input_format(str.as_str())?,
            None => {
                return Err(DataFusionError::Internal(String::from(
                    "input format not existed",
                )));
            }
        };

        let data_cols = Self::extract_field_schemas(&sd.cols)?;
        let table_partition_cols = Self::extract_field_schemas(&table.partition_keys)?;

        let serde_properties: HashMap<String, String> = sd
            .serde_info
            .as_ref()
            .and_then(|s| s.parameters.as_ref())
            .map(|p| {
                p.iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect()
            })
            .unwrap_or_default();

        Ok(Self {
            table_location,
            input_format,
            table_schema: TableSchema::new(
                Arc::new(Schema::new(data_cols)),
                table_partition_cols,
            ),
            serde_properties,
        })
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

    fn extract_field_schemas(field_schemas: &Option<Vec<FieldSchema>>) -> Result<Vec<Arc<Field>>> {
        let field_schemas = if let Some(field_schemas) = field_schemas {
            field_schemas
        } else {
            return Ok(Vec::new());
        };
        let fields: Result<Vec<(String, String)>> = field_schemas.iter().map(|field_schema| {
            let name: String = match field_schema.name.as_ref() {
                Some(s) => s.to_string(),
                None => {
                    return Err(DataFusionError::Internal("FieldSchema's name not existed".to_string()))
                }
            };
            let ty: String = match field_schema.r#type.as_ref() {
                Some(s) => s.to_string(),
                None => {
                    return Err(DataFusionError::Internal("FieldSchema's type not existed".to_string()))
                }
            };
            Ok((name, ty))
        }).collect();

        let fields: Result<Vec<Arc<Field>>> = fields?.iter().map(|(name, ty)| {
           Ok(Arc::new(Field::new(name, hive_type_to_arrow_type(ty)?, true)))
        }).collect();
        fields
    }
}
