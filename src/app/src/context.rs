use crate::catalog::CatalogManager;
use datafusion::common::Result;
use dobbydb_common::runtime::RuntimeManager;
use std::sync::Arc;

pub struct DobbyDbContext {
    pub catalog_manager: Arc<CatalogManager>,
    pub runtime_manager: Arc<RuntimeManager>,
    pub default_catalog: Option<String>,
    pub default_schema: Option<String>,
}

impl Default for DobbyDbContext {
    fn default() -> Self {
        Self::new(None).expect("default context initialization should not fail")
    }
}

impl DobbyDbContext {
    pub fn new(config_path: Option<&str>) -> Result<Self> {
        let mut catalog_manager = CatalogManager::new();
        if let Some(config_path) = config_path {
            catalog_manager.load_config(config_path)?;
        }
        let runtime_manager = RuntimeManager::default();
        Ok(Self {
            catalog_manager: Arc::new(catalog_manager),
            runtime_manager: Arc::new(runtime_manager),
            default_catalog: None,
            default_schema: None,
        })
    }
}
