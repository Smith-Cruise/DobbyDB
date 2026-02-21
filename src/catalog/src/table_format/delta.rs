use crate::storage::StorageCredential;
use datafusion::catalog::TableProvider;
use datafusion::common::TableReference;
use datafusion::common::{DataFusionError, Result};
use deltalake::DeltaTableBuilder;
use std::collections::HashMap;
use std::sync::Arc;
use url::Url;

pub struct DeltaTableProviderFactory {}

impl DeltaTableProviderFactory {
    pub async fn try_create_table_provider(
        _table_reference: &TableReference,
        table_location: &str,
        storage_credential: Option<StorageCredential>,
    ) -> Result<Arc<dyn TableProvider>> {
        let storage_options = if let Some(storage_credential) = &storage_credential {
            storage_credential.build_delta_storage_options()
        } else {
            HashMap::new()
        };
        let table_url =
            Url::parse(table_location).map_err(|e| DataFusionError::External(Box::new(e)))?;
        let builder = DeltaTableBuilder::from_url(table_url)
            .map_err(|e| DataFusionError::External(Box::new(e)))?
            .with_allow_http(true)
            .with_storage_options(storage_options);
        let delta_table = builder
            .load()
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let delta_scan = delta_table.table_provider().build().await?;
        Ok(Arc::new(delta_scan))
    }
}
