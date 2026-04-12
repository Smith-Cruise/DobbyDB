use crate::glue_catalog::{GlueCatalog, GlueCatalogConfig};
use crate::hms_catalog::{HMSCatalog, HMSCatalogConfig};
use datafusion::catalog::{AsyncCatalogProvider, AsyncCatalogProviderList, CatalogProviderList};
use datafusion::common::Result;
use datafusion::error::DataFusionError;
use deltalake::logstore::{logstore_factories, object_store_factories};
use deltalake_aws::S3LogStoreFactory;
use deltalake_aws::storage::S3ObjectStoreFactory;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, OnceLock, RwLock};
use async_trait::async_trait;
use url::Url;
use crate::internal_catalog::{InternalCatalog, INTERNAL_CATALOG};

#[derive(Serialize, Deserialize)]
struct CatalogConfigs {
    hms: Option<Vec<HMSCatalogConfig>>,
    glue: Option<Vec<GlueCatalogConfig>>,
}

#[derive(Debug, Clone)]
pub enum CatalogConfig {
    HMS(HMSCatalogConfig),
    GLUE(GlueCatalogConfig),
}

static CATALOG_MANAGER: OnceLock<RwLock<CatalogManager>> = OnceLock::new();

pub fn get_catalog_manager() -> &'static RwLock<CatalogManager> {
    CATALOG_MANAGER.get_or_init(|| RwLock::new(CatalogManager::default()))
}

#[derive(Debug, Default)]
pub struct CatalogManager {
    catalogs: HashMap<String, CatalogConfig>,
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

    pub fn get_all_catalogs(&self) -> Vec<(String, CatalogConfig)> {
        self.catalogs
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }
}

pub async fn register_catalogs_into_catalog_provider(
    catalog_provider: Arc<dyn CatalogProviderList>,
    catalogs: Vec<(String, CatalogConfig)>,
) -> Result<()> {
    for (key, value) in catalogs {
        match value {
            CatalogConfig::HMS(config) => {
                let hms_catalog = HMSCatalog::try_new(&Arc::new(config)).await?;
                catalog_provider.register_catalog(key, Arc::new(hms_catalog));
            }
            CatalogConfig::GLUE(config) => {
                let glue_catalog = GlueCatalog::try_new(&Arc::new(config)).await?;
                catalog_provider.register_catalog(key, Arc::new(glue_catalog));
            }
        }
    }
    Ok(())
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

pub struct DobbyDbCatalogProviderList {
}

impl DobbyDbCatalogProviderList {
    pub fn new() -> DobbyDbCatalogProviderList {
        Self {}
    }
}

#[async_trait]
impl AsyncCatalogProviderList for DobbyDbCatalogProviderList {
    async fn catalog(&self, catalog_name: &str) -> Result<Option<Arc<dyn AsyncCatalogProvider>>> {
        if catalog_name == INTERNAL_CATALOG {
            return Ok(Some(Arc::new(InternalCatalog::new())))
        }
        let catalog_manager = get_catalog_manager().read().unwrap();
        let catalog = if let Some(catalog) = catalog_manager.get_catalog(catalog_name) {
            catalog.clone()
        } else {
            return Ok(None);
        };
        match catalog {
            CatalogConfig::HMS(hms_catalog) => {
                Ok(Some(Arc::new(HMSCatalog::new(&Arc::new(hms_catalog)))))
            }
            CatalogConfig::GLUE(_) => {
                todo!()
            }
        }
    }
}