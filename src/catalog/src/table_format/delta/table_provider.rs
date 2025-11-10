use datafusion::common::TableReference;
use datafusion::error::DataFusionError;
use deltalake::{DeltaTable, DeltaTableBuilder, DeltaTableError};
use std::collections::HashMap;
use url::Url;

pub struct DeltaTableProvider {
    pub(crate) delta_table: DeltaTable,
}

impl DeltaTableProvider {
    pub async fn try_new(
        table_reference: &TableReference,
        table_location: &str,
        properties: HashMap<String, String>,
    ) -> Result<Self, DataFusionError> {
        let table_url =
            Url::parse(table_location).map_err(|e| DataFusionError::External(Box::new(e)))?;
        let builder = DeltaTableBuilder::from_uri(table_url)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(DeltaTableProvider {
            delta_table: builder
                .load()
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?,
        })
    }
}
