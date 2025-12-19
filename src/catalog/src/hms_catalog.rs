use crate::catalog::CatalogConfig;
use crate::storage::StorageCredential;
use crate::table_format::table_provider_factory::{split_table_name, TableProviderFactory};
use async_trait::async_trait;
use datafusion::catalog::{CatalogProvider, SchemaProvider, TableProvider};
use datafusion::common::TableReference;
use datafusion::error::DataFusionError;
use hive_metastore::{
    GetTableRequest, ThriftHiveMetastoreClient, ThriftHiveMetastoreClientBuilder,
};
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::net::ToSocketAddrs;
use std::ops::Deref;
use std::sync::Arc;
use volo_thrift::MaybeException;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HMSCatalogConfig {
    pub name: String,
    #[serde(rename = "metastore-uri")]
    pub metastore_uri: String,
    #[serde(flatten)]
    pub storage_credential: Option<StorageCredential>,
}

fn build_hms_client(
    config: &Arc<HMSCatalogConfig>,
) -> Result<ThriftHiveMetastoreClient, DataFusionError> {
    let address = config
        .metastore_uri
        .as_str()
        .to_socket_addrs()
        .map_err(|e| DataFusionError::External(Box::new(e)))?
        .next()
        .ok_or_else(|| {
            DataFusionError::Configuration(format!("invalid address: {}", config.metastore_uri))
        })?;
    let client = ThriftHiveMetastoreClientBuilder::new("hms")
        .address(address)
        .make_codec(volo_thrift::codec::default::DefaultMakeCodec::buffered())
        .build();
    Ok(client)
}

/// Format a thrift exception into iceberg error.
pub fn from_thrift_exception<T, E: Debug>(
    value: MaybeException<T, E>,
) -> Result<T, DataFusionError> {
    match value {
        MaybeException::Ok(v) => Ok(v),
        MaybeException::Exception(err) => Err(DataFusionError::Internal(format!(
            "operation failed for hitting thrift error: {:?}",
            err
        ))),
    }
}

#[derive(Debug)]
pub struct HMSCatalog {
    _config: Arc<HMSCatalogConfig>,
    schemas: HashMap<String, Arc<dyn SchemaProvider>>,
}

impl HMSCatalog {
    pub async fn try_new(config: &Arc<HMSCatalogConfig>) -> Result<Self, DataFusionError> {
        let hms_client = build_hms_client(config)?;
        let all_database_names = hms_client
            .get_all_databases()
            .await
            .map(from_thrift_exception)
            .map_err(|e| DataFusionError::External(e.into()))??;

        let mut schemas: HashMap<String, Arc<dyn SchemaProvider>> = HashMap::new();
        for schema_name in all_database_names {
            let schema_provider =
                HMSSchema::try_new(&hms_client, config, schema_name.as_str()).await?;
            schemas.insert(schema_name.to_string(), Arc::new(schema_provider));
        }
        Ok(Self {
            _config: config.clone(),
            schemas,
        })
    }
}

impl CatalogProvider for HMSCatalog {
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
struct HMSSchema {
    config: Arc<HMSCatalogConfig>,
    schema_name: String,
    table_names: HashSet<String>,
}

impl HMSSchema {
    pub async fn try_new(
        hms_client: &ThriftHiveMetastoreClient,
        config: &Arc<HMSCatalogConfig>,
        schema_name: &str,
    ) -> Result<Self, DataFusionError> {
        let all_tables = hms_client
            .get_all_tables(schema_name.to_string().into())
            .await
            .map(from_thrift_exception)
            .map_err(|e| DataFusionError::External(e.into()))??;

        let mut table_names: HashSet<String> = HashSet::new();
        for table_name in all_tables {
            table_names.insert(table_name.to_string());
        }

        Ok(HMSSchema {
            config: config.clone(),
            schema_name: schema_name.to_string(),
            table_names,
        })
    }
}

#[async_trait]
impl SchemaProvider for HMSSchema {
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

        let hms_client = build_hms_client(&self.config)?;
        let get_table_request = GetTableRequest {
            db_name: self.schema_name.clone().into(),
            tbl_name: table_name.to_string().into(),
            capabilities: None,
        };
        let hms_table = hms_client
            .get_table_req(get_table_request)
            .await
            .map(from_thrift_exception)
            .map_err(|e| DataFusionError::External(e.into()))??
            .table;

        let table_reference = TableReference::full(
            self.config.name.as_str(),
            self.schema_name.as_str(),
            table_name,
        );

        let mut hms_table_properties: HashMap<String, String> = HashMap::new();
        match &hms_table.parameters {
            Some(parameters) => {
                for (k, v) in parameters {
                    hms_table_properties.insert(k.to_string(), v.to_string());
                }
            }
            None => {
                return Err(DataFusionError::Internal(
                    "hms table's parameters are missing".to_string(),
                ));
            }
        };
        let table_provider = TableProviderFactory::try_new_table_provider(
            &table_reference,
            metadata_table_name,
            &hms_table_properties,
            CatalogConfig::HMS(self.config.deref().clone()),
        )
        .await?;
        Ok(Some(table_provider))
    }

    fn table_exist(&self, name: &str) -> bool {
        self.table_names.contains(name)
    }
}
