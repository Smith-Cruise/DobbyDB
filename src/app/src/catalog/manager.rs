use crate::context::DobbyDbContext;
use crate::glue_catalog::{GlueCatalog, GlueCatalogConfig};
use crate::hms_catalog::{HMSCatalog, HMSCatalogConfig};
use crate::internal_catalog::{INTERNAL_CATALOG, InternalCatalog};
use async_trait::async_trait;
use datafusion::catalog::{AsyncCatalogProvider, AsyncCatalogProviderList};
use datafusion::common::Result;
use datafusion::error::DataFusionError;
use dobbydb_common::runtime::RuntimeManager;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Serialize, Deserialize)]
pub struct CatalogConfigs {
    pub hms: Option<Vec<HMSCatalogConfig>>,
    pub glue: Option<Vec<GlueCatalogConfig>>,
}

#[allow(clippy::upper_case_acronyms)]
#[derive(Debug, Clone)]
pub enum CatalogConfig {
    Internal,
    HMS(HMSCatalogConfig),
    GLUE(GlueCatalogConfig),
}

#[derive(Debug, Clone)]
pub struct CatalogManager {
    catalogs: HashMap<String, CatalogConfig>,
}

impl Default for CatalogManager {
    fn default() -> Self {
        Self::new()
    }
}

impl CatalogManager {
    pub fn new() -> Self {
        let mut catalogs = HashMap::new();
        catalogs.insert(INTERNAL_CATALOG.to_string(), CatalogConfig::Internal);
        Self { catalogs }
    }
    fn add_catalog(&mut self, catalog_name: &str, catalog_config: CatalogConfig) -> Result<()> {
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

    fn build_catalog_provider(
        &self,
        catalog_name: &str,
    ) -> Result<Box<dyn DobbyDbCatalogProvider + Send + Sync>> {
        let catalog_config = self
            .get_catalog(catalog_name)
            .ok_or_else(|| DataFusionError::Plan(format!("unknown catalog {}", catalog_name)))?;
        let dobbydb_context = Arc::new(DobbyDbContext {
            catalog_manager: Arc::new(self.clone()),
            runtime_manager: Arc::new(RuntimeManager::default()),
        });

        match catalog_config {
            CatalogConfig::Internal => Ok(Box::new(InternalCatalog::new(dobbydb_context))),
            CatalogConfig::HMS(hms_catalog) => Ok(Box::new(HMSCatalog::new(
                dobbydb_context,
                Arc::new(hms_catalog.clone()),
            ))),
            CatalogConfig::GLUE(glue_catalog) => Ok(Box::new(GlueCatalog::new(
                dobbydb_context,
                Arc::new(glue_catalog.clone()),
            ))),
        }
    }

    pub fn catalog_exists(&self, catalog_name: &str) -> bool {
        self.catalogs.contains_key(catalog_name)
    }

    pub async fn list_schema_names(&self, catalog_name: &str) -> Result<Vec<String>> {
        self.build_catalog_provider(catalog_name)?
            .list_schema_names()
            .await
    }

    pub async fn list_table_names(
        &self,
        catalog_name: &str,
        schema_name: &str,
    ) -> Result<Vec<String>> {
        self.build_catalog_provider(catalog_name)?
            .list_table_names(schema_name)
            .await
    }

    pub async fn schema_exist(&self, catalog_name: &str, schema_name: &str) -> Result<bool> {
        self.build_catalog_provider(catalog_name)?
            .schema_exist(schema_name)
            .await
    }

    pub async fn table_exists(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<bool> {
        self.build_catalog_provider(catalog_name)?
            .table_exist(table_name, schema_name)
            .await
    }
}

pub struct DobbyDbCatalogProviderList {
    dobbydb_context: Arc<DobbyDbContext>,
}

impl DobbyDbCatalogProviderList {
    pub fn new(dobbydb_context: Arc<DobbyDbContext>) -> DobbyDbCatalogProviderList {
        Self { dobbydb_context }
    }
}

#[async_trait]
impl AsyncCatalogProviderList for DobbyDbCatalogProviderList {
    async fn catalog(&self, catalog_name: &str) -> Result<Option<Arc<dyn AsyncCatalogProvider>>> {
        let catalog_config = if let Some(catalog_config) = self
            .dobbydb_context
            .catalog_manager
            .get_catalog(catalog_name)
        {
            catalog_config.clone()
        } else {
            return Ok(None);
        };

        match catalog_config {
            CatalogConfig::Internal => Ok(Some(Arc::new(
                crate::internal_catalog::InternalCatalog::new(self.dobbydb_context.clone()),
            ))),
            CatalogConfig::HMS(hms_catalog) => Ok(Some(Arc::new(HMSCatalog::new(
                self.dobbydb_context.clone(),
                Arc::new(hms_catalog),
            )))),
            CatalogConfig::GLUE(glue_catalog) => Ok(Some(Arc::new(GlueCatalog::new(
                self.dobbydb_context.clone(),
                Arc::new(glue_catalog),
            )))),
        }
    }
}

#[async_trait]
pub trait DobbyDbCatalogProvider {
    async fn list_schema_names(&self) -> Result<Vec<String>>;

    async fn list_table_names(&self, schema_name: &str) -> Result<Vec<String>>;

    async fn schema_exist(&self, schema_name: &str) -> Result<bool>;

    async fn table_exist(&self, table_name: &str, schema_name: &str) -> Result<bool>;
}
