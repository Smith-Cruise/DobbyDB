mod table_provider;

use crate::table_format::delta::table_provider::DeltaTableProvider;
use datafusion::catalog::TableProvider;
use datafusion::common::{DataFusionError, TableReference};
use std::collections::HashMap;
use std::sync::Arc;

pub struct DeltaTableProviderFactory {}

impl DeltaTableProviderFactory {
    pub async fn try_create_table_provider(
        metadata_location: &str,
        table_reference: &TableReference,
        properties: HashMap<String, String>,
    ) -> Result<Arc<dyn TableProvider>, DataFusionError> {
        let delta_table_provider =
            DeltaTableProvider::try_new(table_reference, metadata_location, properties).await?;
        Ok(Arc::new(delta_table_provider))
    }
}