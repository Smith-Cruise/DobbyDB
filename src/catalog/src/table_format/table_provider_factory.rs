use crate::catalog::CatalogConfig;
use crate::storage::{convert_storage_to_delta, convert_storage_to_iceberg};
use crate::table_format::delta::DeltaTableProviderFactory;
use crate::table_format::iceberg::IcebergTableProviderFactory;
use datafusion::catalog::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::sql::TableReference;
use std::collections::HashMap;
use std::sync::Arc;

pub(crate) struct TableProviderFactory {}

impl TableProviderFactory {
    pub async fn try_new_table_provider(
        table_reference: &TableReference,
        metadata_table_name: Option<&str>,
        table_properties: &HashMap<String, String>,
        catalog_config: CatalogConfig,
    ) -> Result<Arc<dyn TableProvider>, DataFusionError> {
        if let Some(iceberg_metadata_location) = table_properties.get("metadata_location") {
            // iceberg
            let mut iceberg_config: HashMap<String, String> = HashMap::new();
            match catalog_config {
                CatalogConfig::GLUE(glue_config) => {
                    if let Some(storage_credentials) = &glue_config.storage_credential {
                        iceberg_config = convert_storage_to_iceberg(storage_credentials);
                    }
                }
                CatalogConfig::HMS(hms_config) => {
                    if let Some(storage_credentials) = &hms_config.storage_credential {
                        iceberg_config = convert_storage_to_iceberg(storage_credentials);
                    }
                }
            }
            return IcebergTableProviderFactory::try_create_table_provider(
                iceberg_metadata_location,
                table_reference,
                metadata_table_name,
                iceberg_config,
            )
            .await;
        }

        if let Some(spark_provider) = table_properties.get("spark.sql.sources.provider") {
            if let Some(table_location) = table_properties.get("location") {
                if spark_provider == "DELTA" {
                    // delta
                    let mut delta_config: HashMap<String, String> = HashMap::new();
                    match catalog_config {
                        CatalogConfig::GLUE(glue_config) => {
                            if let Some(storage_credentials) = &glue_config.storage_credential {
                                delta_config = convert_storage_to_delta(storage_credentials);
                            }
                        }
                        CatalogConfig::HMS(hms_config) => {
                            if let Some(storage_credentials) = &hms_config.storage_credential {
                                delta_config = convert_storage_to_delta(storage_credentials);
                            }
                        }
                    }
                    return DeltaTableProviderFactory::try_create_table_provider(
                        table_reference,
                        table_location,
                        delta_config,
                    )
                    .await;
                }
            }
        }

        Err(DataFusionError::NotImplemented(
            "not implemented table format".to_string(),
        ))
    }
}

pub fn split_table_name(tbl_name: &str) -> (&str, Option<&str>) {
    match tbl_name.split_once("$") {
        Some((tmp_table_name, tmp_metadata_table_name)) => {
            (tmp_table_name, Some(tmp_metadata_table_name))
        }
        None => (tbl_name, None),
    }
}
