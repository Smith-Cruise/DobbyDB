use crate::glue_catalog::{GlueCatalog, GlueCatalogConfig};
use crate::hms_catalog::{HMSCatalog, HMSCatalogConfig};
use datafusion::catalog::CatalogProviderList;
use datafusion::common::Result;
use datafusion::error::DataFusionError;
use deltalake::logstore::{logstore_factories, object_store_factories};
use deltalake_aws::S3LogStoreFactory;
use deltalake_aws::storage::S3ObjectStoreFactory;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, OnceLock, RwLock};
use url::Url;

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
    CATALOG_MANAGER.get_or_init(|| RwLock::new(CatalogManager::default()))
}

#[derive(Debug)]
pub struct CatalogManager {
    catalogs: HashMap<String, CatalogConfig>,
}

impl Default for CatalogManager {
    fn default() -> Self {
        Self {
            catalogs: HashMap::default(),
        }
    }
}

impl CatalogManager {
    fn register_catalog(
        &mut self,
        catalog_name: &str,
        catalog_config: CatalogConfig,
    ) -> Result<()> {
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

    pub fn load_config(&mut self, config_path: &str) -> Result<()> {
        let config = std::fs::read_to_string(config_path)?;
        let configs: CatalogConfigs = toml::from_str(&config).map_err(|e| {
            DataFusionError::Configuration(format!("Failed to parse config: {}", e))
        })?;

        if let Some(catalogs) = configs.hms {
            for hms_catalog in catalogs {
                self.register_catalog(&hms_catalog.name, CatalogConfig::HMS(hms_catalog.clone()))?;
            }
        }

        if let Some(catalogs) = configs.glue {
            for glue_catalog in catalogs {
                self.register_catalog(
                    &glue_catalog.name,
                    CatalogConfig::GLUE(glue_catalog.clone()),
                )?;
            }
        }

        register_something();
        Ok(())
    }

    pub fn get_catalog(&self, catalog_name: &str) -> Option<&CatalogConfig> {
        self.catalogs.get(catalog_name)
    }

    pub async fn register_into_catalog_provider_list(
        &self,
        catalog_list: Arc<dyn CatalogProviderList>,
    ) -> Result<()> {
        for (key, value) in &self.catalogs {
            match value {
                CatalogConfig::HMS(config) => {
                    let hms_catalog = HMSCatalog::try_new(&Arc::new(config.clone())).await?;
                    catalog_list.register_catalog(key.to_string(), Arc::new(hms_catalog));
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

fn register_something() {
    {
        // register delta
        let object_stores = Arc::new(S3ObjectStoreFactory::default());
        let log_stores = Arc::new(S3LogStoreFactory::default());
        for scheme in ["s3", "s3a", "oss"].iter() {
            let url = Url::parse(&format!("{scheme}://")).unwrap();
            object_store_factories().insert(url.clone(), object_stores.clone());
            logstore_factories().insert(url.clone(), log_stores.clone());
        }
    }
}
