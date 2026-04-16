use std::sync::Arc;
use dobbydb_catalog::catalog::CatalogManager;
use dobbydb_common::runtime::RuntimeManager;
use datafusion::common::Result;

pub mod parser;
pub mod session;
pub mod statements;


pub struct DobbyDbContext {
    pub catalog_manager: Arc<CatalogManager>,
    pub runtime_manager: Arc<RuntimeManager>,
}

impl DobbyDbContext {
    pub fn default() -> Result<Self> {
        Self::new(None)
    }

    pub fn new(config_path: Option<&str>) -> Result<Self> {
        let mut catalog_manager = CatalogManager::new();
        if let Some(config_path) = config_path {
            catalog_manager.load_config(config_path)?;
        }
        let runtime_manager= RuntimeManager::default();
        Ok(Self {
            catalog_manager: Arc::new(catalog_manager),
            runtime_manager: Arc::new(runtime_manager),
        })
    }
}