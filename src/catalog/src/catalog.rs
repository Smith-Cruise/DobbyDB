use crate::glue_catalog::GlueCatalogConfig;
use crate::hms_catalog::{HMSCatalog, HMSCatalogConfig};
use crate::internal_catalog::{InternalCatalog, INTERNAL_CATALOG};
use async_trait::async_trait;
use datafusion::catalog::{AsyncCatalogProvider, AsyncCatalogProviderList};
use datafusion::common::Result;
use datafusion::error::DataFusionError;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Serialize, Deserialize)]
pub struct CatalogConfigs {
    pub hms: Option<Vec<HMSCatalogConfig>>,
    pub glue: Option<Vec<GlueCatalogConfig>>,
}

#[derive(Debug, Clone)]
pub enum CatalogConfig {
    Internal,
    HMS(HMSCatalogConfig),
    GLUE(GlueCatalogConfig),
}

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

pub struct DobbyDbCatalogProviderList {
    catalog_manager: Arc<CatalogManager>
}

impl DobbyDbCatalogProviderList {
    pub fn new(catalog_manager: Arc<CatalogManager>) -> DobbyDbCatalogProviderList {
        Self {
            catalog_manager
        }
    }
}

#[async_trait]
impl AsyncCatalogProviderList for DobbyDbCatalogProviderList {
    async fn catalog(&self, catalog_name: &str) -> Result<Option<Arc<dyn AsyncCatalogProvider>>> {
        let catalog_config = if let Some(catalog_config) = self.catalog_manager.get_catalog(catalog_name) {
            catalog_config.clone()
        } else {
            return Ok(None);
        };

        match catalog_config {
            CatalogConfig::Internal => {
                Ok(Some(Arc::new(InternalCatalog::new(self.catalog_manager.clone()))))
            }
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
    async fn list_schema_names(&self) -> Result<Vec<String>>;

    async fn list_table_names(&self, schema_name: &str) -> Result<Vec<String>>;
}