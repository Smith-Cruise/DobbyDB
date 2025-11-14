mod table_provider;

use crate::table_format::delta::table_provider::DeltaTableProvider;
use datafusion::catalog::TableProvider;
use datafusion::common::{DataFusionError, TableReference};
use std::collections::HashMap;
use std::sync::Arc;

pub struct DeltaTableProviderFactory {}

impl DeltaTableProviderFactory {
    pub async fn try_create_table_provider(
        table_reference: &TableReference,
        table_location: &str,
        properties: HashMap<String, String>,
    ) -> Result<Arc<dyn TableProvider>, DataFusionError> {
        let delta_table_provider =
            DeltaTableProvider::try_new(table_reference, table_location, properties).await?;
        Ok(Arc::new(delta_table_provider))
    }
}