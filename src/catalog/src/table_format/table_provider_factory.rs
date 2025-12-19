use crate::catalog::CatalogConfig;
use crate::storage::StorageCredential;
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
            let storage_credential: Option<StorageCredential>;
            match catalog_config {
                CatalogConfig::GLUE(glue_config) => {
                    storage_credential = glue_config.storage_credential.clone();
                }
                CatalogConfig::HMS(hms_config) => {
                    storage_credential = hms_config.storage_credential.clone();
                }
            }
            return IcebergTableProviderFactory::try_create_table_provider(
                iceberg_metadata_location,
                table_reference,
                metadata_table_name,
                storage_credential,
            )
            .await;
        }

        if let Some(spark_provider) = table_properties.get("spark.sql.sources.provider") {
            if let Some(table_location) = table_properties.get("location") {
                if spark_provider == "DELTA" {
                    // delta
                    let storage_credential: Option<StorageCredential>;
                    match catalog_config {
                        CatalogConfig::GLUE(glue_config) => {
                            storage_credential = glue_config.storage_credential.clone();
                        }
                        CatalogConfig::HMS(hms_config) => {
                            storage_credential = hms_config.storage_credential.clone();
                        }
                    }
                    return DeltaTableProviderFactory::try_create_table_provider(
                        table_reference,
                        table_location,
                        storage_credential,
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
