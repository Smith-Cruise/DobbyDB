use crate::glue_catalog::{GlueCatalog, GlueCatalogConfig};
use crate::hms_catalog::HMSCatalogConfig;
use datafusion::catalog::{CatalogProvider, CatalogProviderList};
use datafusion::error::DataFusionError;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock, RwLock};

pub trait CatalogConfigTrait: Send + Sync {
    fn convert_iceberg_config(&self) -> HashMap<String, String>;
}

#[derive(Serialize, Deserialize)]
struct CatalogConfigs {
    hms: Option<Vec<HMSCatalogConfig>>,
    glue: Option<Vec<GlueCatalogConfig>>,
}

#[derive(Debug)]
pub enum CatalogConfig {
    HMS(HMSCatalogConfig),
    GLUE(GlueCatalogConfig),
}

static CATALOG_MANAGER: OnceLock<RwLock<CatalogManager>> = OnceLock::new();

pub fn get_catalog_manager() -> &'static RwLock<CatalogManager> {
    CATALOG_MANAGER.get_or_init(|| RwLock::new(CatalogManager::new()))
}

#[derive(Debug)]
pub struct CatalogManager {
    catalogs: HashMap<String, CatalogConfig>,
}

impl CatalogManager {
    pub fn new() -> CatalogManager {
        CatalogManager {
            catalogs: HashMap::new(),
        }
    }

    fn register_catalog(
        &mut self,
        catalog_name: &str,
        catalog_config: CatalogConfig,
    ) -> Result<(), DataFusionError> {
        if self.catalogs.contains_key(catalog_name) {
            Err(DataFusionError::Configuration(format!(
                "Catalog {} already exists",
                catalog_name
            )))
        } else {
            self.catalogs
                .insert(catalog_name.to_string(), catalog_config);
            Ok(())
        }
    }

    pub fn load_config(&mut self, config_path: &str) -> Result<(), DataFusionError> {
        let config = std::fs::read_to_string(config_path)?;
        let configs: CatalogConfigs = toml::from_str(&config).map_err(|e| {
            DataFusionError::Configuration(format!("Failed to parse config: {}", e))
        })?;

        match &configs.hms {
            Some(catalogs) => {
                for hms_catalog in catalogs {
                    self.register_catalog(
                        &hms_catalog.name,
                        CatalogConfig::HMS(hms_catalog.clone()),
                    )?;
                }
            }
            _ => {}
        }

        match &configs.glue {
            Some(catalogs) => {
                for glue_catalog in catalogs {
                    self.register_catalog(
                        &glue_catalog.name,
                        CatalogConfig::GLUE(glue_catalog.clone()),
                    )?;
                }
            }
            None => {}
        }

        Ok(())
    }

    pub fn get_catalogs(&self) -> &HashMap<String, CatalogConfig> {
        &self.catalogs
    }

    pub async fn register_into_catalog_list(
        &self,
        catalog_list: Arc<dyn CatalogProviderList>,
    ) -> Result<(), DataFusionError> {
        for (key, value) in &self.catalogs {
            match value {
                CatalogConfig::HMS(config) => {
                    return Err(DataFusionError::NotImplemented(
                        "hms not implemented".to_string(),
                    ));
                }
                CatalogConfig::GLUE(config) => {
                    let glue_catalog = GlueCatalog::try_new(&Arc::new(config.clone())).await?;
                    catalog_list.register_catalog(key.to_string(), Arc::new(glue_catalog));
                }
            }
        }
        Ok(())
    }
}