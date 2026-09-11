use crate::catalog::table_format::hive::parse_statistics_from_table_properties;
use crate::catalog::{CatalogConfig, LakeletCatalogProvider};
use crate::context::LakeletContext;
use crate::table_format::TableFormat;
use crate::table_format::hive::HMSTableSchemaBuilder;
use crate::table_format::hive::hive_partition::HivePartition;
use crate::table_format::hive::hive_storage_info::HiveStorageInfo;
use crate::table_format::table_provider_factory::{
    TableProviderBuilder, deduce_table_format, parse_table_reference,
};
use async_trait::async_trait;
use datafusion::catalog::{AsyncCatalogProvider, AsyncSchemaProvider, TableProvider};
use datafusion::common::TableReference;
use datafusion::common::{Result, Statistics};
use datafusion::error::DataFusionError;
use hive_metastore::{
    GetTableRequest, ThriftHiveMetastoreClient, ThriftHiveMetastoreClientBuilder,
    ThriftHiveMetastoreGetDatabaseException, ThriftHiveMetastoreGetTableReqException,
};
use lakelet_storage::storage::Storage;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;
use std::net::ToSocketAddrs;
use std::ops::Deref;
use std::sync::Arc;
use tokio::sync::OnceCell;
use volo_thrift::MaybeException;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HMSCatalogConfig {
    pub name: String,
    #[serde(rename = "metastore-uri")]
    pub metastore_uri: String,
    #[serde(flatten, default)]
    pub storage: Storage,
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
    lakelet_context: Arc<LakeletContext>,
    config: Arc<HMSCatalogConfig>,
    // Lazily built once; the catalog provider list caches HMSCatalog itself,
    // so volo's pooled transports stay warm across statements.
    client: OnceCell<ThriftHiveMetastoreClient>,
}

impl HMSCatalog {
    pub fn new(lakelet_context: Arc<LakeletContext>, config: Arc<HMSCatalogConfig>) -> Self {
        Self {
            lakelet_context,
            config,
            client: OnceCell::new(),
        }
    }

    async fn client(&self) -> Result<ThriftHiveMetastoreClient> {
        // On failure the cell stays empty, so a transient DNS error is
        // retried by the next call.
        self.client
            .get_or_try_init(|| async { self.build_hms_client() })
            .await
            // The volo-thrift client is cheap to clone; clones share the
            // pooled transport.
            .cloned()
    }

    fn build_hms_client(&self) -> Result<ThriftHiveMetastoreClient> {
        let address = self
            .config
            .metastore_uri
            .as_str()
            .to_socket_addrs()
            .map_err(|e| DataFusionError::External(Box::new(e)))?
            .next()
            .ok_or_else(|| {
                DataFusionError::Configuration(format!(
                    "invalid address: {}",
                    self.config.metastore_uri
                ))
            })?;
        let client = ThriftHiveMetastoreClientBuilder::new("hms")
            .address(address)
            .make_codec(volo_thrift::codec::default::DefaultMakeCodec::buffered())
            .build();
        Ok(client)
    }
}

#[async_trait]
impl LakeletCatalogProvider for HMSCatalog {
    async fn list_schema_names(&self) -> Result<Vec<String>> {
        let hms_client = self.client().await?;
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
        let hms_client = self.client().await?;
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
        let hms_client = self.client().await?;
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
        let hms_client = self.client().await?;
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
            self.client().await?,
            self.lakelet_context.clone(),
            self.config.clone(),
            schema_name,
        )?)))
    }
}

struct HMSSchema {
    client: ThriftHiveMetastoreClient,
    lakelet_context: Arc<LakeletContext>,
    config: Arc<HMSCatalogConfig>,
    schema_name: String,
}

impl HMSSchema {
    pub fn new(
        client: ThriftHiveMetastoreClient,
        lakelet_context: Arc<LakeletContext>,
        config: Arc<HMSCatalogConfig>,
        schema_name: &str,
    ) -> Result<Self> {
        Ok(Self {
            client,
            lakelet_context,
            config,
            schema_name: schema_name.to_string(),
        })
    }
}

#[async_trait]
impl AsyncSchemaProvider for HMSSchema {
    async fn table(&self, tbl_name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        let (table_name, metadata_table_type) = parse_table_reference(tbl_name)?;

        let hms_client = &self.client;
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
            table_name.as_str(),
        );

        let mut hms_table_properties: HashMap<String, String> = HashMap::new();
        if let Some(parameters) = &hms_table.parameters {
            for (k, v) in parameters {
                hms_table_properties.insert(k.to_string(), v.to_string());
            }
        }

        let storage_descriptor = hms_table.sd.as_ref().ok_or_else(|| {
            DataFusionError::Internal("Storage descriptor not existed".to_string())
        })?;
        // TODO: For Paimon Hive catalogs with location-in-properties=true, derive the
        // table location from table properties before falling back to sd.location.
        let table_location = storage_descriptor
            .location
            .as_ref()
            .map(ToString::to_string)
            .ok_or_else(|| DataFusionError::Internal("location not existed".to_string()))?;
        let table_format = deduce_table_format(&hms_table_properties)?;
        let (hive_storage_info, hive_partitions) = if table_format == TableFormat::Hive {
            let table_schema = HMSTableSchemaBuilder::new(&hms_table).build()?;
            let mut table_statistics = Statistics::new_unknown(table_schema.table_schema());
            if metadata_table_type.is_none() {
                parse_statistics_from_table_properties(
                    &mut table_statistics,
                    &hms_table_properties,
                );
            }
            let hive_storage_info = HiveStorageInfo::try_new_from_hms_table(
                table_schema,
                table_statistics,
                &hms_table,
            )?;
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
            self.lakelet_context.clone(),
            table_location,
            table_reference,
            hms_table_properties,
            table_format,
            CatalogConfig::HMS(self.config.deref().clone()),
        );
        let table_provider_builder = table_provider_builder
            .with_metadata_table_type(metadata_table_type)
            .with_hive_storage_info(hive_storage_info)
            .with_hive_partitions(hive_partitions);
        Ok(Some(table_provider_builder.build().await?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_hms_config_without_storage_block() {
        let config: HMSCatalogConfig = toml::from_str(
            r#"
            name = "hms_1"
            metastore-uri = "127.0.0.1:9083"
        "#,
        )
        .unwrap();
        assert_eq!(config.name, "hms_1");
        // No storage block: hdfs needs none, and the object-store schemes
        // fall back to OpenDAL's credential chain (covered in
        // lakelet_storage::storage).
        assert!(config.storage.s3_storage.is_none());
        assert!(config.storage.oss_storage.is_none());
        assert!(config.storage.build_operator("hdfs", "nn:8020").is_ok());
    }

    #[test]
    fn test_parse_hms_config_with_storage_block() {
        let config: HMSCatalogConfig = toml::from_str(
            r#"
            name = "hms_1"
            metastore-uri = "127.0.0.1:9083"
            s3-storage = { endpoint = "http://127.0.0.1:9000", region = "us-east-1", access-key = "ak", secret-key = "sk" }
        "#,
        )
        .unwrap();
        assert!(config.storage.build_operator("s3", "bucket").is_ok());
    }

    #[tokio::test]
    async fn test_hms_client_init_failure_is_retried() {
        let catalog = HMSCatalog::new(
            Arc::new(LakeletContext::default()),
            Arc::new(HMSCatalogConfig {
                name: "hms_1".to_string(),
                // Missing port: to_socket_addrs fails without any network access.
                metastore_uri: "not-a-valid-uri".to_string(),
                storage: Storage::default(),
            }),
        );
        assert!(catalog.client().await.is_err());
        // The cell stays empty after a failed init, so the next call retries.
        assert!(catalog.client.get().is_none());
        assert!(catalog.client().await.is_err());
    }
}
