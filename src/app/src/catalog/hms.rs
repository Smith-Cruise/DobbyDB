use crate::catalog::{CatalogConfig, DobbyDbCatalogProvider};
use crate::context::DobbyDbContext;
use crate::table_format::TableFormat;
use crate::table_format::hive::hive_partition::HivePartition;
use crate::table_format::hive::hive_storage_info::HiveStorageInfo;
use crate::table_format::table_provider_factory::{
    TableProviderBuilder, deduce_table_format, split_table_name,
};
use async_trait::async_trait;
use datafusion::catalog::{AsyncCatalogProvider, AsyncSchemaProvider, TableProvider};
use datafusion::common::Result;
use datafusion::common::TableReference;
use datafusion::error::DataFusionError;
use dobbydb_storage::storage::Storage;
use hive_metastore::{
    GetTableRequest, ThriftHiveMetastoreClient, ThriftHiveMetastoreClientBuilder,
    ThriftHiveMetastoreGetDatabaseException, ThriftHiveMetastoreGetTableReqException,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
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
    pub storage: Option<Storage>,
}

fn build_hms_client(config: &Arc<HMSCatalogConfig>) -> Result<ThriftHiveMetastoreClient> {
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
pub fn from_thrift_exception<T, E: Debug>(value: MaybeException<T, E>) -> Result<T> {
    match value {
        MaybeException::Ok(v) => Ok(v),
        MaybeException::Exception(err) => Err(DataFusionError::Internal(format!(
            "operation failed for hitting thrift error: {:?}",
            err
        ))),
    }
}

pub struct HMSCatalog {
    dobbydb_context: Arc<DobbyDbContext>,
    config: Arc<HMSCatalogConfig>,
}

impl HMSCatalog {
    pub fn new(dobbydb_context: Arc<DobbyDbContext>, config: Arc<HMSCatalogConfig>) -> Self {
        Self {
            dobbydb_context,
            config,
        }
    }
}

#[async_trait]
impl DobbyDbCatalogProvider for HMSCatalog {
    async fn list_schema_names(&self) -> Result<Vec<String>> {
        let hms_client = build_hms_client(&self.config)?;
        let all_database_names = hms_client
            .get_all_databases()
            .await
            .map(from_thrift_exception)
            .map_err(|e| DataFusionError::External(e.into()))??;
        Ok(all_database_names
            .into_iter()
            .map(|name| name.to_string())
            .collect())
    }

    async fn list_table_names(&self, schema_name: &str) -> Result<Vec<String>> {
        let hms_client = build_hms_client(&self.config)?;
        let all_tables = hms_client
            .get_all_tables(schema_name.to_string().into())
            .await
            .map(from_thrift_exception)
            .map_err(|e| DataFusionError::External(e.into()))??;
        Ok(all_tables
            .into_iter()
            .map(|name| name.to_string())
            .collect())
    }

    async fn schema_exist(&self, schema_name: &str) -> Result<bool> {
        let hms_client = build_hms_client(&self.config)?;
        match hms_client
            .get_database(schema_name.to_string().into())
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?
        {
            MaybeException::Ok(_) => Ok(true),
            MaybeException::Exception(ThriftHiveMetastoreGetDatabaseException::O1(_)) => Ok(false),
            MaybeException::Exception(err) => Err(DataFusionError::Internal(format!(
                "operation failed for hitting thrift error: {:?}",
                err
            ))),
        }
    }

    async fn table_exist(&self, table_name: &str, schema_name: &str) -> Result<bool> {
        let hms_client = build_hms_client(&self.config)?;
        let get_table_request = GetTableRequest {
            db_name: schema_name.to_string().into(),
            tbl_name: table_name.to_string().into(),
            capabilities: None,
        };
        match hms_client
            .get_table_req(get_table_request)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?
        {
            MaybeException::Ok(_) => Ok(true),
            MaybeException::Exception(ThriftHiveMetastoreGetTableReqException::O2(_)) => Ok(false),
            MaybeException::Exception(err) => Err(DataFusionError::Internal(format!(
                "operation failed for hitting thrift error: {:?}",
                err
            ))),
        }
    }
}

#[async_trait]
impl AsyncCatalogProvider for HMSCatalog {
    async fn schema(&self, schema_name: &str) -> Result<Option<Arc<dyn AsyncSchemaProvider>>> {
        Ok(Some(Arc::new(HMSSchema::new(
            self.dobbydb_context.clone(),
            self.config.clone(),
            schema_name,
        )?)))
    }
}

struct HMSSchema {
    dobbydb_context: Arc<DobbyDbContext>,
    config: Arc<HMSCatalogConfig>,
    schema_name: String,
}

impl HMSSchema {
    pub fn new(
        dobbydb_context: Arc<DobbyDbContext>,
        config: Arc<HMSCatalogConfig>,
        schema_name: &str,
    ) -> Result<Self> {
        Ok(Self {
            dobbydb_context,
            config,
            schema_name: schema_name.to_string(),
        })
    }
}

#[async_trait]
impl AsyncSchemaProvider for HMSSchema {
    async fn table(&self, tbl_name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        let (table_name, metadata_table_name) = split_table_name(tbl_name);

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
        if let Some(parameters) = &hms_table.parameters {
            for (k, v) in parameters {
                hms_table_properties.insert(k.to_string(), v.to_string());
            }
        }

        let table_format = deduce_table_format(&hms_table_properties)?;
        let (hive_storage_info, hive_partitions) = if table_format == TableFormat::Hive {
            let hive_storage_info = HiveStorageInfo::try_new_from_hms_table(&hms_table)?;
            let hive_partitions = if !hive_storage_info
                .table_schema
                .table_partition_cols()
                .is_empty()
            {
                let partitions = hms_client
                    .get_partitions(
                        self.schema_name.clone().into(),
                        table_name.to_string().into(),
                        i16::MAX,
                    )
                    .await
                    .map(from_thrift_exception)
                    .map_err(|e| DataFusionError::External(e.into()))??;
                partitions
                    .iter()
                    .map(HivePartition::try_new_from_hms_partition)
                    .collect::<Result<Vec<_>, _>>()?
            } else {
                vec![]
            };
            (Some(hive_storage_info), Some(hive_partitions))
        } else {
            (None, None)
        };

        let table_provider_builder = TableProviderBuilder::new(
            self.dobbydb_context.clone(),
            table_reference,
            hms_table_properties,
            table_format,
            CatalogConfig::HMS(self.config.deref().clone()),
        );
        let table_provider_builder = table_provider_builder
            .with_table_metadata_table_name(metadata_table_name.map(|t| t.to_string()))
            .with_hive_storage_info(hive_storage_info)
            .with_hive_partitions(hive_partitions);
        Ok(Some(table_provider_builder.build().await?))
    }
}
