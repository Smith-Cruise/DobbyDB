use crate::catalog::CatalogConfigTrait;
use crate::constants::ICEBERG_METADATA_LOCATION;
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
        catalog_config: &dyn CatalogConfigTrait,
    ) -> Result<Arc<dyn TableProvider>, DataFusionError> {
        if let Some(iceberg_metadata_location) = table_properties.get(ICEBERG_METADATA_LOCATION) {
            // iceberg
            let iceberg_config = catalog_config.convert_iceberg_config();
            IcebergTableProviderFactory::try_create_table_provider(
                iceberg_metadata_location,
                table_reference,
                metadata_table_name,
                &iceberg_config,
            )
            .await
        } else {
            Err(DataFusionError::NotImplemented(
                "not implemented table format".to_string(),
            ))
        }
    }
}
