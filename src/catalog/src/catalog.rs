use crate::glue_catalog::{GlueCatalog, GlueCatalogConfig};
use crate::hms_catalog::{HMSCatalog, HMSCatalogConfig};
use crate::internal_catalog::{InternalCatalog, INTERNAL_CATALOG};
use async_trait::async_trait;
use datafusion::catalog::{AsyncCatalogProvider, AsyncCatalogProviderList, CatalogProviderList};
use datafusion::common::Result;
use datafusion::error::DataFusionError;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, OnceLock, RwLock};

#[derive(Serialize, Deserialize)]
struct CatalogConfigs {
    hms: Option<Vec<HMSCatalogConfig>>,
    glue: Option<Vec<GlueCatalogConfig>>,
}

#[derive(Debug, Clone)]
pub enum CatalogConfig {
    Internal,
    HMS(HMSCatalogConfig),
    GLUE(GlueCatalogConfig),
}

// static CATALOG_MANAGER: OnceLock<RwLock<CatalogManager>> = OnceLock::new();
//
// pub fn get_catalog_manager() -> &'static RwLock<CatalogManager> {
//     CATALOG_MANAGER.get_or_init(|| RwLock::new(CatalogManager::default()))
// }

#[derive(Debug)]
pub struct CatalogManager {
    catalogs: HashMap<String, CatalogConfig>,
}

impl CatalogManager {
    pub fn new() -> Self {
        let mut catalogs = HashMap::new();
        catalogs.insert(INTERNAL_CATALOG.to_string(), CatalogConfig::Internal);
        Self {
            catalogs
        }
    }
    fn add_catalog(
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
                self.add_catalog(&hms_catalog.name, CatalogConfig::HMS(hms_catalog.clone()))?;
            }
        }

        if let Some(catalogs) = configs.glue {
            for glue_catalog in catalogs {
                self.add_catalog(
                    &glue_catalog.name,
                    CatalogConfig::GLUE(glue_catalog.clone()),
                )?;
            }
        }
        Ok(())
    }

    pub fn get_catalog(&self, catalog_name: &str) -> Option<&CatalogConfig> {
        self.catalogs.get(catalog_name)
    }

    pub fn list_catalogs(&self) -> Vec<(String, CatalogConfig)> {
        self.catalogs
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }
}

// pub async fn register_catalogs_into_catalog_provider(
//     catalog_provider: Arc<dyn CatalogProviderList>,
//     catalogs: Vec<(String, CatalogConfig)>,
// ) -> Result<()> {
//     for (key, value) in catalogs {
//         match value {
//             CatalogConfig::HMS(config) => {
//                 let hms_catalog = HMSCatalog::try_new(&Arc::new(config)).await?;
//                 catalog_provider.register_catalog(key, Arc::new(hms_catalog));
//             }
//             CatalogConfig::GLUE(config) => {
//                 let glue_catalog = GlueCatalog::try_new(&Arc::new(config)).await?;
//                 catalog_provider.register_catalog(key, Arc::new(glue_catalog));
//             }
//         }
//     }
//     Ok(())
// }

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

#[async_trait]
pub trait DobbyDbCatalogProvider {
    async fn list_schemas(&self) -> Result<Vec<String>>;

    async fn list_tables(&self, schema_name: &str) -> Result<Vec<String>>;
}