use crate::catalog::{CatalogConfig, DobbyDbCatalogProvider};
use crate::table_format::TableFormat;
use crate::table_format::hive::hive_partition::HivePartition;
use crate::table_format::hive::hive_storage_info::HiveStorageInfo;
use crate::table_format::table_provider_factory::{
    TableProviderBuilder, deduce_table_format, split_table_name,
};
use async_trait::async_trait;
use aws_config::Region;
use aws_sdk_glue::Client;
use aws_sdk_glue::config::Credentials;
use datafusion::catalog::{AsyncCatalogProvider, AsyncSchemaProvider, TableProvider};
use datafusion::common::Result;
use datafusion::common::TableReference;
use datafusion::error::DataFusionError;
use dobbydb_storage::storage::Storage;
use iceberg::inspect::MetadataTableType;
use serde::{Deserialize, Serialize};
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
    pub storage: Option<Storage>,
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
    Client::new(&aws_config)
}

#[derive(Debug)]
pub struct GlueCatalog {
    config: Arc<GlueCatalogConfig>,
}

impl GlueCatalog {
    pub fn new(config: &Arc<GlueCatalogConfig>) -> Self {
        Self {
            config: config.clone(),
        }
    }
}
#[async_trait]
impl AsyncCatalogProvider for GlueCatalog {
    async fn schema(&self, schema_name: &str) -> Result<Option<Arc<dyn AsyncSchemaProvider>>> {
        Ok(Some(Arc::new(GlueSchema::new(&self.config, schema_name))))
    }
}

#[derive(Debug)]
pub struct GlueSchema {
    config: Arc<GlueCatalogConfig>,
    schema_name: String,
}

impl GlueSchema {
    pub fn new(config: &Arc<GlueCatalogConfig>, schema_name: &str) -> Self {
        Self {
            config: config.clone(),
            schema_name: schema_name.to_string(),
        }
    }
}

#[async_trait]
impl AsyncSchemaProvider for GlueSchema {
    async fn table(&self, tbl_name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        let (table_name, metadata_table_name) = split_table_name(tbl_name);

        if let Some(metadata_table_name) = metadata_table_name {
            if MetadataTableType::try_from(metadata_table_name).is_err() {
                return Ok(None);
            }
        }

        let glue_client = build_glue_client(&self.config).await;
        let resp = match glue_client
            .get_table()
            .database_name(&self.schema_name)
            .name(table_name)
            .send()
            .await
        {
            Ok(resp) => resp,
            Err(err)
                if err
                    .as_service_error()
                    .is_some_and(|err| err.is_entity_not_found_exception()) =>
            {
                return Ok(None);
            }
            Err(err) => return Err(DataFusionError::External(Box::new(err))),
        };

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
            Some(parameters) => parameters.clone(),
            None => {
                return Err(DataFusionError::Internal(
                    "glue table's parameters are missing".to_string(),
                ));
            }
        };

        let table_format = deduce_table_format(&glue_table_properties)?;
        let (hive_storage_info, hive_partitions) = if table_format == TableFormat::Hive {
            let hive_storage_info = HiveStorageInfo::try_new_from_glue_table(&glue_table)?;
            let hive_partitions = if !hive_storage_info
                .table_schema
                .table_partition_cols()
                .is_empty()
            {
                let paginator = glue_client
                    .get_partitions()
                    .database_name(&self.schema_name)
                    .table_name(table_name)
                    .into_paginator()
                    .send();
                tokio::pin!(paginator);

                let mut partitions = Vec::new();
                while let Some(page) = paginator.next().await {
                    let page = page.map_err(|e| DataFusionError::External(Box::new(e)))?;
                    partitions.extend(
                        page.partitions()
                            .iter()
                            .map(HivePartition::try_new_from_glue_partition)
                            .collect::<Result<Vec<_>>>()?,
                    );
                }
                partitions
            } else {
                vec![]
            };
            (Some(hive_storage_info), Some(hive_partitions))
        } else {
            (None, None)
        };

        let table_provider_builder = TableProviderBuilder::new(
            table_reference,
            glue_table_properties,
            table_format,
            CatalogConfig::GLUE(self.config.deref().clone()),
        );
        let table_provider_builder = table_provider_builder
            .with_table_metadata_table_name(metadata_table_name.map(|t| t.to_string()))
            .with_hive_storage_info(hive_storage_info)
            .with_hive_partitions(hive_partitions);

        Ok(Some(table_provider_builder.build().await?))
    }
}

#[async_trait]
impl DobbyDbCatalogProvider for GlueCatalog {
    async fn list_schema_names(&self) -> Result<Vec<String>> {
        let glue_client = build_glue_client(&self.config).await;
        let paginator = glue_client.get_databases().into_paginator().send();
        tokio::pin!(paginator);

        let mut schema_names = Vec::new();
        while let Some(page) = paginator.next().await {
            let page = page.map_err(|e| DataFusionError::External(Box::new(e)))?;
            for database in page.database_list() {
                schema_names.push(database.name.clone());
            }
        }

        Ok(schema_names)
    }

    async fn list_table_names(&self, schema_name: &str) -> Result<Vec<String>> {
        let glue_client = build_glue_client(&self.config).await;
        let paginator = glue_client
            .get_tables()
            .database_name(schema_name)
            .into_paginator()
            .send();
        tokio::pin!(paginator);

        let mut table_names = Vec::new();
        while let Some(page) = paginator.next().await {
            let page = page.map_err(|e| DataFusionError::External(Box::new(e)))?;
            for table in page.table_list() {
                table_names.push(table.name.clone());
            }
        }

        Ok(table_names)
    }

    async fn schema_exist(&self, schema_name: &str) -> Result<bool> {
        let glue_client = build_glue_client(&self.config).await;
        match glue_client.get_database().name(schema_name).send().await {
            Ok(_) => Ok(true),
            Err(err)
                if err
                    .as_service_error()
                    .is_some_and(|err| err.is_entity_not_found_exception()) =>
            {
                Ok(false)
            }
            Err(err) => Err(DataFusionError::External(Box::new(err))),
        }
    }

    async fn table_exist(&self, table_name: &str, schema_name: &str) -> Result<bool> {
        let glue_client = build_glue_client(&self.config).await;
        match glue_client
            .get_table()
            .database_name(schema_name)
            .name(table_name)
            .send()
            .await
        {
            Ok(_) => Ok(true),
            Err(err)
                if err
                    .as_service_error()
                    .is_some_and(|err| err.is_entity_not_found_exception()) =>
            {
                Ok(false)
            }
            Err(err) => Err(DataFusionError::External(Box::new(err))),
        }
    }
}
