use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use crate::catalog::CatalogConfigTrait;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HMSCatalogConfig {
    pub name: String,
}

impl CatalogConfigTrait for HMSCatalogConfig {
    fn convert_iceberg_config(&self) -> HashMap<String, String> {
        HashMap::new()
    }
}