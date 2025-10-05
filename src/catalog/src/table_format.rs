use crate::catalog::CatalogConfigTrait;
use crate::constants::ICEBERG_METADATA_LOCATION;
use crate::table_format::iceberg::IcebergTableFormat;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::DataFusionError;
use datafusion::sql::TableReference;
use ::iceberg::arrow::schema_to_arrow_schema;
use std::collections::HashMap;
use std::sync::Arc;

pub mod iceberg;

#[derive(Debug)]
pub(crate) enum TableFormat {
    Iceberg(IcebergTableFormat),
}

pub(crate) async fn try_new_table_format(
    table_reference: &TableReference,
    catalog_config: &dyn CatalogConfigTrait,
    table_properties: &HashMap<String, String>,
) -> Result<TableFormat, DataFusionError> {
    if let Some(iceberg_metadata_location) = table_properties.get(ICEBERG_METADATA_LOCATION) {
        // iceberg
        let iceberg_config = catalog_config.convert_iceberg_config();
        let iceberg_format = IcebergTableFormat::try_new(
            table_reference,
            iceberg_metadata_location,
            &iceberg_config,
        )
        .await?;
        Ok(TableFormat::Iceberg(iceberg_format))
    } else {
        Err(DataFusionError::NotImplemented(
            "not implemented table format".to_string(),
        ))
    }
}

pub(crate) fn try_new_table_schema(
    table_format: &TableFormat,
) -> Result<SchemaRef, DataFusionError> {
    match table_format {
        TableFormat::Iceberg(iceberg_table_format) => {
            let schema = schema_to_arrow_schema(
                iceberg_table_format
                    .static_table
                    .metadata()
                    .current_schema(),
            )
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
            Ok(Arc::new(schema))
        }
    }
}