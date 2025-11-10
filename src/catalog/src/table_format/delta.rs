mod table_provider;

use std::collections::HashMap;
use std::sync::Arc;
use datafusion::catalog::TableProvider;
use datafusion::common::{DataFusionError, TableReference};
use crate::table_format::iceberg::metadata_table_provider::IcebergMetadataTableProvider;
use crate::table_format::iceberg::table_provider::IcebergTableProvider;

pub struct DeltaTableProviderFactory {}

impl DeltaTableProviderFactory {
    pub async fn try_create_table_provider(
        metadata_location: &str,
        table_reference: &TableReference,
        metadata_table_name: Option<&str>,
        properties: HashMap<String, String>,
    ) -> Result<Arc<dyn TableProvider>, DataFusionError> {
        let iceberg_table_provider =
            IcebergTableProvider::try_new(table_reference, metadata_location, properties).await?;
        if let Some(metadata_table_name) = metadata_table_name {
            let metadata_table_provider = IcebergMetadataTableProvider::try_new(
                iceberg_table_provider.table,
                metadata_table_name,
            )?;
            return Ok(Arc::new(metadata_table_provider));
        }
        Ok(Arc::new(iceberg_table_provider))
    }
}