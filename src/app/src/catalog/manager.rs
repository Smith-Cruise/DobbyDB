use crate::catalog::iceberg_rest::{IcebergRestCatalog, IcebergRestCatalogConfig};
use crate::catalog::statistics::StatisticsManager;
use crate::context::LakeletContext;
use crate::glue_catalog::{GlueCatalog, GlueCatalogConfig};
use crate::hms_catalog::{HMSCatalog, HMSCatalogConfig};
use crate::internal_catalog::{INTERNAL_CATALOG, InternalCatalog};
use crate::paimon_fs_catalog::{PaimonFSCatalog, PaimonFSCatalogConfig};
use async_trait::async_trait;
use datafusion::catalog::{AsyncCatalogProvider, AsyncCatalogProviderList};
use datafusion::common::Result;
use datafusion::error::DataFusionError;
use lakelet_common::runtime::RuntimeManager;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(Default, Serialize, Deserialize)]
pub struct CatalogConfigs {
    #[serde(rename = "iceberg-rest")]
    pub iceberg_rest: Option<Vec<IcebergRestCatalogConfig>>,
    pub hms: Option<Vec<HMSCatalogConfig>>,
    pub glue: Option<Vec<GlueCatalogConfig>>,
    #[serde(rename = "paimon-fs")]
    pub paimon_fs: Option<Vec<PaimonFSCatalogConfig>>,
}

#[allow(clippy::upper_case_acronyms)]
#[derive(Debug, Clone)]
pub enum CatalogConfig {
    Internal,
    IcebergRest(IcebergRestCatalogConfig),
    HMS(HMSCatalogConfig),
    GLUE(GlueCatalogConfig),
    PaimonFS(PaimonFSCatalogConfig),
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

    pub fn load_catalogs(&mut self, catalogs: &CatalogConfigs) -> Result<()> {
        if let Some(ref hms_catalogs) = catalogs.hms {
            for hms_catalog in hms_catalogs {
                self.add_catalog(&hms_catalog.name, CatalogConfig::HMS(hms_catalog.clone()))?;
            }
        }

        if let Some(ref glue_catalogs) = catalogs.glue {
            for glue_catalog in glue_catalogs {
                self.add_catalog(
                    &glue_catalog.name,
                    CatalogConfig::GLUE(glue_catalog.clone()),
                )?;
            }
        }

        if let Some(ref paimon_fs_catalogs) = catalogs.paimon_fs {
            for paimon_fs_catalog in paimon_fs_catalogs {
                self.add_catalog(
                    &paimon_fs_catalog.name,
                    CatalogConfig::PaimonFS(paimon_fs_catalog.clone()),
                )?;
            }
        }

        if let Some(rest_catalogs) = &catalogs.iceberg_rest {
            for config in rest_catalogs {
                self.add_catalog(&config.name, CatalogConfig::IcebergRest(config.clone()))?;
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
    ) -> Result<Box<dyn LakeletCatalogProvider + Send + Sync>> {
        let catalog_config = self
            .get_catalog(catalog_name)
            .ok_or_else(|| DataFusionError::Plan(format!("unknown catalog {}", catalog_name)))?;
        let lakelet_context = Arc::new(LakeletContext {
            server_config: Default::default(),
            catalog_manager: Arc::new(self.clone()),
            statistics_manager: Arc::new(StatisticsManager::default()),
            runtime_manager: Arc::new(RuntimeManager::default()),
            default_catalog: None,
            default_schema: None,
        });

        match catalog_config {
            CatalogConfig::IcebergRest(config) => {
                Ok(Box::new(IcebergRestCatalog::new(Arc::new(config.clone()))))
            }
            CatalogConfig::Internal => Ok(Box::new(InternalCatalog::new(lakelet_context))),
            CatalogConfig::HMS(hms_catalog) => Ok(Box::new(HMSCatalog::new(
                lakelet_context,
                Arc::new(hms_catalog.clone()),
            ))),
            CatalogConfig::GLUE(glue_catalog) => Ok(Box::new(GlueCatalog::new(
                lakelet_context,
                Arc::new(glue_catalog.clone()),
            ))),
            CatalogConfig::PaimonFS(paimon_fs_catalog) => Ok(Box::new(PaimonFSCatalog::try_new(
                lakelet_context,
                Arc::new(paimon_fs_catalog.clone()),
            )?)),
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

pub struct LakeletCatalogProviderList {
    lakelet_context: Arc<LakeletContext>,
    // One catalog provider per configured catalog, built on first reference.
    // Providers own their metastore clients, so caching them here keeps
    // clients (and their connection pools) alive across statements.
    catalogs: Mutex<HashMap<String, Arc<dyn AsyncCatalogProvider>>>,
}

impl LakeletCatalogProviderList {
    pub fn new(lakelet_context: Arc<LakeletContext>) -> LakeletCatalogProviderList {
        Self {
            lakelet_context,
            catalogs: Mutex::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl AsyncCatalogProviderList for LakeletCatalogProviderList {
    async fn catalog(&self, catalog_name: &str) -> Result<Option<Arc<dyn AsyncCatalogProvider>>> {
        let catalog_config = if let Some(catalog_config) = self
            .lakelet_context
            .catalog_manager
            .get_catalog(catalog_name)
        {
            catalog_config.clone()
        } else {
            return Ok(None);
        };

        // All provider constructors are synchronous, so the lock is never
        // held across an await point.
        let mut catalogs = self.catalogs.lock().unwrap();
        if let Some(catalog) = catalogs.get(catalog_name) {
            return Ok(Some(catalog.clone()));
        }

        let catalog: Arc<dyn AsyncCatalogProvider> = match catalog_config {
            CatalogConfig::IcebergRest(config) => {
                Arc::new(IcebergRestCatalog::new(Arc::new(config)))
            }
            CatalogConfig::Internal => Arc::new(InternalCatalog::new(self.lakelet_context.clone())),
            CatalogConfig::HMS(hms_catalog) => Arc::new(HMSCatalog::new(
                self.lakelet_context.clone(),
                Arc::new(hms_catalog),
            )),
            CatalogConfig::GLUE(glue_catalog) => Arc::new(GlueCatalog::new(
                self.lakelet_context.clone(),
                Arc::new(glue_catalog),
            )),
            CatalogConfig::PaimonFS(paimon_fs_catalog) => Arc::new(PaimonFSCatalog::try_new(
                self.lakelet_context.clone(),
                Arc::new(paimon_fs_catalog),
            )?),
        };
        catalogs.insert(catalog_name.to_string(), catalog.clone());
        Ok(Some(catalog))
    }
}

#[async_trait]
pub trait LakeletCatalogProvider {
    async fn list_schema_names(&self) -> Result<Vec<String>>;

    async fn list_table_names(&self, schema_name: &str) -> Result<Vec<String>>;

    async fn schema_exist(&self, schema_name: &str) -> Result<bool>;

    async fn table_exist(&self, table_name: &str, schema_name: &str) -> Result<bool>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_load_format_catalogs_from_config() {
        let configs: CatalogConfigs = toml::from_str(
            r#"
            [[paimon-fs]]
            name = "paimon_fs_1"
            warehouse = "s3://bucket/warehouse"

            [[iceberg-rest]]
            name = "iceberg_prod"
            uri = "http://localhost:8181"
            s3-storage = { region = "us-east-1", access-key = "ak", secret-key = "sk" }
        "#,
        )
        .unwrap();

        let mut catalog_manager = CatalogManager::new();
        catalog_manager.load_catalogs(&configs).unwrap();
        assert!(catalog_manager.catalog_exists("paimon_fs_1"));
        assert!(matches!(
            catalog_manager.get_catalog("paimon_fs_1"),
            Some(CatalogConfig::PaimonFS(_))
        ));
        let Some(CatalogConfig::IcebergRest(rest)) = catalog_manager.get_catalog("iceberg_prod")
        else {
            panic!("iceberg_prod should be an Iceberg REST catalog");
        };
        assert!(rest.storage.s3_storage.is_some());
    }

    #[test]
    fn test_load_catalogs_rejects_duplicate_names() {
        let configs: CatalogConfigs = toml::from_str(
            r#"
            [[hms]]
            name = "dup"
            metastore-uri = "127.0.0.1:9083"

            [[paimon-fs]]
            name = "dup"
            warehouse = "s3://bucket/warehouse"
        "#,
        )
        .unwrap();

        let mut catalog_manager = CatalogManager::new();
        let result = catalog_manager.load_catalogs(&configs);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("already exists"));
    }

    #[tokio::test]
    async fn test_catalog_provider_list_caches_providers() {
        let provider_list = LakeletCatalogProviderList::new(Arc::new(LakeletContext::default()));
        let first = provider_list
            .catalog(INTERNAL_CATALOG)
            .await
            .unwrap()
            .unwrap();
        let second = provider_list
            .catalog(INTERNAL_CATALOG)
            .await
            .unwrap()
            .unwrap();
        // Repeated resolution returns the same cached provider instance.
        assert!(Arc::ptr_eq(&first, &second));
        assert!(provider_list.catalog("missing").await.unwrap().is_none());
    }
}
