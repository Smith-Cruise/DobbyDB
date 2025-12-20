mod table_provider;

use crate::storage::StorageCredential;
use crate::table_format::delta::table_provider::DeltaTableProvider;
use datafusion::catalog::TableProvider;
use datafusion::common::Result;
use datafusion::common::TableReference;
use std::sync::Arc;

pub struct DeltaTableProviderFactory {}

impl DeltaTableProviderFactory {
    pub async fn try_create_table_provider(
        table_reference: &TableReference,
        table_location: &str,
        storage_credential: Option<StorageCredential>,
    ) -> Result<Arc<dyn TableProvider>> {
        let delta_table_provider =
            DeltaTableProvider::try_new(table_reference, table_location, storage_credential)
                .await?;
        Ok(Arc::new(delta_table_provider))
    }
}
