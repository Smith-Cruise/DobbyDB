use datafusion::common::HashMap;
use datafusion::error::DataFusionError;
use crate::constants::ICEBERG_METADATA_LOCATION;
use crate::table_format::iceberg::IcebergTableFormat;

pub mod iceberg;



pub enum TableFormat {
    Iceberg(IcebergTableFormat),
    Invalid
}

pub fn try_new_table_format(properties: &HashMap<String, String>) -> Result<TableFormat, DataFusionError> {
    if properties.contains_key(ICEBERG_METADATA_LOCATION) {
        // iceberg
        Ok(TableFormat::Invalid)
    } else {
        Err(DataFusionError::NotImplemented("not implemented table format".to_string()))
    }
}