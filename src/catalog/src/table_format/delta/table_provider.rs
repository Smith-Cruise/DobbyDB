use crate::storage::StorageCredential;
use datafusion::common::Result;
use datafusion::common::TableReference;
use datafusion::error::DataFusionError;
use deltalake::{DeltaTable, DeltaTableBuilder};
use std::collections::HashMap;
use url::Url;

#[derive(Debug)]
pub struct DeltaTableProvider {}

impl DeltaTableProvider {
    pub async fn try_new(
        _table_reference: &TableReference,
        table_location: &str,
        storage_credential: Option<StorageCredential>,
    ) -> Result<DeltaTable> {
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
        Ok(delta_table)
    }
}
