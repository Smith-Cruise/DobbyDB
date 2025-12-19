use crate::catalog::CatalogConfig;
use crate::storage::StorageCredential;
use crate::table_format::table_provider_factory::{split_table_name, TableProviderFactory};
use async_trait::async_trait;
use aws_config::Region;
use aws_sdk_glue::config::Credentials;
use aws_sdk_glue::Client;
use datafusion::catalog::{CatalogProvider, SchemaProvider, TableProvider};
use datafusion::common::TableReference;
use datafusion::error::DataFusionError;
use iceberg::inspect::MetadataTableType;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::ops::Deref;
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlueCatalogConfig {
    pub name: String,
    #[serde(rename = "aws-glue-region")]
    pub aws_glue_region: Option<String>,
    #[serde(rename = "aws-glue-access-key")]
    pub aws_glue_access_key: Option<String>,
    #[serde(rename = "aws-glue-secret-key")]
    pub aws_glue_secret_key: Option<String>,
    #[serde(flatten)]
    pub storage_credential: Option<StorageCredential>,
}

async fn build_glue_client(config: &GlueCatalogConfig) -> Client {
    let mut aws_config = aws_config::defaults(aws_config::BehaviorVersion::latest());
    if let (Some(access_key), Some(secret_key)) =
        (&config.aws_glue_access_key, &config.aws_glue_secret_key)
    {
        let credential_provider = Credentials::new(access_key, secret_key, None, None, "DobbyDB");
        aws_config = aws_config.credentials_provider(credential_provider);
    }
    if let Some(region) = &config.aws_glue_region {
        aws_config = aws_config.region(Region::new(region.clone()));
    }
    let aws_config = aws_config.load().await;
    let glue_client = Client::new(&aws_config);
    glue_client
}

#[derive(Debug)]
pub struct GlueCatalog {
    _config: Arc<GlueCatalogConfig>,
    schemas: HashMap<String, Arc<dyn SchemaProvider>>,
}

impl GlueCatalog {
    pub async fn try_new(config: &Arc<GlueCatalogConfig>) -> Result<Self, DataFusionError> {
        let glue_client = build_glue_client(config).await;
        let mut schemas: HashMap<String, Arc<dyn SchemaProvider>> = HashMap::new();
        let dbs = glue_client
            .get_databases()
            .send()
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        for database in dbs.database_list {
            let glue_schema = GlueSchema::try_new(&glue_client, config, &database.name).await?;
            schemas.insert(database.name.clone(), Arc::new(glue_schema));
        }
        Ok(GlueCatalog {
            _config: config.clone(),
            schemas,
        })
    }
}

impl CatalogProvider for GlueCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        self.schemas.keys().cloned().collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.schemas.get(name).cloned()
    }
}

#[derive(Debug)]
pub struct GlueSchema {
    config: Arc<GlueCatalogConfig>,
    schema_name: String,
    table_names: HashSet<String>,
}

impl GlueSchema {
    pub async fn try_new(
        glue_client: &Client,
        config: &Arc<GlueCatalogConfig>,
        schema_name: &str,
    ) -> Result<Self, DataFusionError> {
        let mut table_names = HashSet::new();
        let resp = glue_client
            .get_tables()
            .database_name(schema_name)
            .send()
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        if let Some(glue_tables) = resp.table_list {
            for glue_table in glue_tables {
                table_names.insert(glue_table.name.clone());
            }
        }

        Ok(Self {
            config: config.clone(),
            schema_name: schema_name.to_string(),
            table_names,
        })
    }
}

#[async_trait]
impl SchemaProvider for GlueSchema {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.table_names.iter().cloned().collect()
    }

    async fn table(
        &self,
        tbl_name: &str,
    ) -> datafusion::common::Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        let (table_name, metadata_table_name) = split_table_name(tbl_name);

        if !self.table_exist(table_name) {
            return Ok(None);
        }

        let glue_client = build_glue_client(&self.config).await;
        let resp = glue_client
            .get_table()
            .database_name(&self.schema_name)
            .name(table_name)
            .send()
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let glue_table = match resp.table {
            Some(glue_table) => glue_table,
            None => return Ok(None),
        };

        let table_reference = TableReference::full(
            self.config.name.as_str(),
            self.schema_name.as_str(),
            glue_table.name.as_str(),
        );
        let glue_table_properties = match &glue_table.parameters {
            Some(parameters) => parameters,
            None => {
                return Err(DataFusionError::Internal(
                    "glue table's parameters are missing".to_string(),
                ));
            }
        };
        let table_provider = TableProviderFactory::try_new_table_provider(
            &table_reference,
            metadata_table_name,
            glue_table_properties,
            CatalogConfig::GLUE(self.config.deref().clone()),
        )
        .await?;
        Ok(Some(table_provider))
    }

    fn table_exist(&self, name: &str) -> bool {
        if let Some((table_name, metadata_table_name)) = name.split_once('$') {
            self.table_names.contains(table_name)
                && MetadataTableType::try_from(metadata_table_name).is_ok()
        } else {
            self.table_names.contains(name)
        }
    }
}
