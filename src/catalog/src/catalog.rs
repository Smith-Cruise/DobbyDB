use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{OnceLock, RwLock};
use datafusion::error::DataFusionError;

#[derive(Serialize, Deserialize)]
struct CatalogConfigs {
    hms: Vec<HMSCatalogConfig>,
    glue: Vec<GlueCatalogConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HMSCatalogConfig {
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlueCatalogConfig {
    pub name: String,
    #[serde(rename = "aws-glue-region")]
    pub aws_glue_region: Option<String>,
    #[serde(rename = "aws-glue-access-key")]
    pub aws_glue_access_key: Option<String>,
    #[serde(rename = "aws-glue-secret-key")]
    pub aws_glue_secret_key: Option<String>,
    #[serde(rename = "aws-s3-region")]
    pub aws_s3_region: Option<String>,
    #[serde(rename = "aws-s3-access-key")]
    pub aws_s3_access_key: Option<String>,
    #[serde(rename = "aws-s3-secret-key")]
    pub aws_s3_secret_key: Option<String>,
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

    fn insert(
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
        for hms_catalog in &configs.hms {
            self.insert(&hms_catalog.name, CatalogConfig::HMS(hms_catalog.clone()))?;
        }

        for glue_catalog in &configs.glue {
            self.insert(
                &glue_catalog.name,
                CatalogConfig::GLUE(glue_catalog.clone()),
            )?;
        }
        Ok(())
    }
    
    pub fn get_catalogs(&self) -> &HashMap<String, CatalogConfig> {
        &self.catalogs
    }
}


