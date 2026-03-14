use crate::catalog::CatalogConfig;
use crate::storage::StorageCredential;
use crate::table_format::TableFormat;
use crate::table_format::hive::hive_partition::HivePartition;
use crate::table_format::hive::hive_storage_info::HiveStorageInfo;
use crate::table_format::table_provider_factory::{TableProviderBuilder, split_table_name};
use async_trait::async_trait;
use datafusion::catalog::{CatalogProvider, SchemaProvider, TableProvider};
use datafusion::common::Result;
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

#[derive(Debug)]
pub struct HMSCatalog {
    _config: Arc<HMSCatalogConfig>,
    schemas: HashMap<String, Arc<dyn SchemaProvider>>,
}

impl HMSCatalog {
    pub async fn try_new(config: &Arc<HMSCatalogConfig>) -> Result<Self> {
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
    ) -> Result<Self> {
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

    async fn table(&self, tbl_name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
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
        if let Some(parameters) = &hms_table.parameters {
            for (k, v) in parameters {
                hms_table_properties.insert(k.to_string(), v.to_string());
            }
        }

        let mut table_provider_builder = TableProviderBuilder::new(
            table_reference,
            hms_table_properties,
            CatalogConfig::HMS(self.config.deref().clone()),
        );
        table_provider_builder = table_provider_builder
            .with_table_metadata_table_name(metadata_table_name.map(|t| t.to_string()));

        if table_provider_builder.deduce_table_format()? == TableFormat::Hive {
            // if it's hive format, provide more
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

            table_provider_builder =
                table_provider_builder.with_hive_storage_info(hive_storage_info);
            table_provider_builder = table_provider_builder.with_hive_partitions(hive_partitions);
        }

        Ok(Some(table_provider_builder.build().await?))

        // let is_iceberg = hms_table_properties.contains_key("metadata_location");
        // let is_delta = hms_table_properties
        //     .get("spark.sql.sources.provider")
        //     .map(|s| s.as_str())
        //     == Some("DELTA");
        //
        // if !is_iceberg && !is_delta {
        //     if let Some(sd) = &hms_table.sd {
        //         if let Some(input_format) = &sd.input_format {
        //             match HiveTableProviderFactory::detect_input_format(input_format.as_str()) {
        //                 Ok(fmt) => {
        //                     let location = sd
        //                         .location
        //                         .as_ref()
        //                         .ok_or_else(|| {
        //                             DataFusionError::Internal(
        //                                 "hive table sd is missing location".to_string(),
        //                             )
        //                         })?
        //                         .to_string();
        //
        //                     let data_cols: Vec<(String, String)> = sd
        //                         .cols
        //                         .as_ref()
        //                         .map(|cols| {
        //                             cols.iter()
        //                                 .filter_map(|f| {
        //                                     let name = f.name.as_ref()?.to_string();
        //                                     let ty = f.r#type.as_ref()?.to_string();
        //                                     Some((name, ty))
        //                                 })
        //                                 .collect()
        //                         })
        //                         .unwrap_or_default();
        //
        //                     let partition_cols: Vec<(String, String)> = hms_table
        //                         .partition_keys
        //                         .as_ref()
        //                         .map(|keys| {
        //                             keys.iter()
        //                                 .filter_map(|f| {
        //                                     let name = f.name.as_ref()?.to_string();
        //                                     let ty = f.r#type.as_ref()?.to_string();
        //                                     Some((name, ty))
        //                                 })
        //                                 .collect()
        //                         })
        //                         .unwrap_or_default();
        //
        //                     let serde_properties: HashMap<String, String> = sd
        //                         .serde_info
        //                         .as_ref()
        //                         .and_then(|s| s.parameters.as_ref())
        //                         .map(|p| {
        //                             p.iter()
        //                                 .map(|(k, v)| (k.to_string(), v.to_string()))
        //                                 .collect()
        //                         })
        //                         .unwrap_or_default();
        //
        //                     let storage_credential = self.config.storage_credential.clone();
        //
        //                     let hive_info = HiveStorageInfo {
        //                         location,
        //                         input_format: fmt,
        //                         data_cols,
        //                         partition_cols: partition_cols.clone(),
        //                         serde_properties,
        //                         storage_credential,
        //                     };
        //
        //                     let hms_partitions = if !partition_cols.is_empty() {
        //                         hms_client
        //                             .get_partitions(
        //                                 self.schema_name.clone().into(),
        //                                 table_name.to_string().into(),
        //                                 i16::MAX,
        //                             )
        //                             .await
        //                             .map(from_thrift_exception)
        //                             .map_err(|e| DataFusionError::External(e.into()))??
        //                     } else {
        //                         vec![]
        //                     };
        //
        //                     let table_provider =
        //                         HiveTableProviderFactory::try_create_table_provider(
        //                             hive_info,
        //                             hms_partitions,
        //                         )
        //                         .await?;
        //                     return Ok(Some(table_provider));
        //                 }
        //                 Err(e) => return Err(e),
        //             }
        //         }
        //     }
        // }
        //
        // let table_provider = TableProviderFactory::try_new_table_provider(
        //     &table_reference,
        //     metadata_table_name,
        //     &hms_table_properties,
        //     CatalogConfig::HMS(self.config.deref().clone()),
        // )
        // .await?;
    }

    fn table_exist(&self, name: &str) -> bool {
        self.table_names.contains(name)
    }
}
