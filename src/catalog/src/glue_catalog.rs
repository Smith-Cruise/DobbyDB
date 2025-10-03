use async_trait::async_trait;
use aws_config::Region;
use aws_sdk_glue::config::Credentials;
use aws_sdk_glue::types::Table;
use aws_sdk_glue::Client;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{CatalogProvider, SchemaProvider, Session, TableProvider};
use datafusion::common::TableReference;
use datafusion::datasource::TableType;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::sync::Arc;
use iceberg::io::{S3_ACCESS_KEY_ID, S3_REGION, S3_SECRET_ACCESS_KEY};
use crate::catalog::CatalogConfigTrait;
use crate::table_format::{try_new_table_format, try_new_table_schema};

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

impl CatalogConfigTrait for GlueCatalogConfig {
    fn convert_iceberg_config(&self) -> HashMap<String, String> {
        let mut map: HashMap<String, String> = HashMap::new();
        if let Some(region) = &self.aws_s3_region {
            map.insert(S3_REGION.into(), region.clone());
        }
        if let Some(access_key) = &self.aws_s3_access_key {
            map.insert(S3_ACCESS_KEY_ID.into(), access_key.clone());
        }
        if let Some(secret_key) = &self.aws_s3_secret_key {
            map.insert(S3_SECRET_ACCESS_KEY.into(), secret_key.clone());
        }
        map
    }
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
    config: Arc<GlueCatalogConfig>,
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
            config: config.clone(),
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
    table_names: HashSet<String>
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
        if !self.table_exist(tbl_name) {
            return Ok(None);
        }

        let glue_client = build_glue_client(&self.config).await;
        let resp = glue_client
            .get_table()
            .database_name(&self.schema_name)
            .name(tbl_name)
            .send()
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let glue_table = match resp.table {
            Some(glue_table) => glue_table,
            None => return Ok(None)
        };
        let built_glue_table = GlueTable::try_new(&self.config, &self.schema_name, &glue_table).await?;
        Ok(Some(Arc::new(built_glue_table)))
    }

    fn table_exist(&self, name: &str) -> bool {
        self.table_names.contains(name)
    }
}

#[derive(Debug)]
pub struct GlueTable {
    table_reference: TableReference,
    table_schema: SchemaRef,
    table_properties: Option<HashMap<String, String>>,
    config: Arc<GlueCatalogConfig>,
}

impl GlueTable {
    pub async fn try_new(
        config: &Arc<GlueCatalogConfig>,
        schema_name: &str,
        glue_table: &Table,
    ) -> Result<Self, DataFusionError> {
        let table_reference =
            TableReference::full(config.name.as_str(), schema_name, glue_table.name.as_str());
        let table_properties = match &glue_table.parameters {
            Some(parameters) => { parameters.clone() },
            None => {
                return Err(DataFusionError::NotImplemented("unsupported glue table".to_string()));
            }
        };
        let table_format = try_new_table_format(&table_reference, &**config, &table_properties).await?;
        let table_schema = try_new_table_schema(&table_format)?;
        Ok(GlueTable {
            table_reference,
            table_schema,
            table_properties: glue_table.parameters.clone(),
            config: config.clone(),
        })
    }
}

#[async_trait]
impl TableProvider for GlueTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.table_schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        todo!()
    }
}
