use datafusion::common::TableReference;
use datafusion::error::DataFusionError;
use deltalake::{DeltaTable, DeltaTableBuilder};
use std::collections::HashMap;
use url::Url;

#[derive(Debug)]
pub struct DeltaTableProvider {
}

impl DeltaTableProvider {
    pub async fn try_new(
        _table_reference: &TableReference,
        table_location: &str,
        _properties: HashMap<String, String>,
    ) -> Result<DeltaTable, DataFusionError> {
        let table_url =
            Url::parse(table_location).map_err(|e| DataFusionError::External(Box::new(e)))?;
        let builder = DeltaTableBuilder::from_uri(table_url)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let delta_table =
            builder
                .load()
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(delta_table)
    }
}

