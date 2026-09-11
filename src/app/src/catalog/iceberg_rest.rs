use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::{AsyncCatalogProvider, AsyncSchemaProvider, TableProvider};
use datafusion::common::{DataFusionError, Result, TableReference};
use iceberg::{Catalog, CatalogBuilder, ErrorKind, NamespaceIdent, TableIdent};
use iceberg_catalog_rest::{
    REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE, RestCatalogBuilder,
};
use serde::{Deserialize, Serialize};
use tokio::sync::OnceCell;

use crate::catalog::LakeletCatalogProvider;
use crate::table_format::iceberg::iceberg_file_io::IcebergStorageFactory;
use crate::table_format::iceberg::iceberg_metadata_table_provider::IcebergMetadataTableProvider;
use crate::table_format::iceberg::iceberg_table_provider::IcebergTableProvider;
use crate::table_format::table_provider_factory::parse_table_reference;
use lakelet_storage::storage::Storage;

// Upstream exports constants for `uri` and `warehouse` only.
/// Bearer token forwarded to the REST catalog.
const REST_CATALOG_PROP_TOKEN: &str = "token";
/// `header.`-prefixed props become request headers; ask the catalog to vend
/// per-table storage credentials.
const ACCESS_DELEGATION_HEADER_PROP: &str = "header.X-Iceberg-Access-Delegation";
const VENDED_CREDENTIALS: &str = "vended-credentials";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IcebergRestCatalogConfig {
    pub name: String,
    pub uri: String,
    pub warehouse: Option<String>,
    pub token: Option<String>,
    #[serde(flatten, default)]
    pub storage: Storage,
}

impl IcebergRestCatalogConfig {
    fn properties(&self) -> HashMap<String, String> {
        let mut props = HashMap::from([
            (REST_CATALOG_PROP_URI.into(), self.uri.clone()),
            (
                ACCESS_DELEGATION_HEADER_PROP.into(),
                VENDED_CREDENTIALS.into(),
            ),
        ]);
        if let Some(warehouse) = &self.warehouse {
            props.insert(REST_CATALOG_PROP_WAREHOUSE.into(), warehouse.clone());
        }
        if let Some(token) = &self.token {
            props.insert(REST_CATALOG_PROP_TOKEN.into(), token.clone());
        }
        props
    }
}

fn to_datafusion_error(error: iceberg::Error) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

fn namespace_from_schema(name: &str) -> Result<NamespaceIdent> {
    if name.split('.').any(str::is_empty) {
        return Err(DataFusionError::Plan(
            "Iceberg namespace components must not be empty".into(),
        ));
    }
    NamespaceIdent::from_strs(name.split('.')).map_err(to_datafusion_error)
}

pub(crate) struct IcebergRestCatalog {
    config: Arc<IcebergRestCatalogConfig>,
    inner: Arc<OnceCell<Arc<dyn Catalog>>>,
}

impl IcebergRestCatalog {
    pub(crate) fn new(config: Arc<IcebergRestCatalogConfig>) -> Self {
        Self {
            config,
            inner: Arc::new(OnceCell::new()),
        }
    }

    async fn client(&self) -> Result<Arc<dyn Catalog>> {
        self.inner
            .get_or_try_init(|| async {
                let client = RestCatalogBuilder::default()
                    .with_storage_factory(Arc::new(IcebergStorageFactory::new(
                        self.config.storage.clone(),
                    )))
                    .load(&self.config.name, self.config.properties())
                    .await
                    .map_err(to_datafusion_error)?;
                Ok(Arc::new(client) as Arc<dyn Catalog>)
            })
            .await
            .cloned()
    }
}

#[async_trait]
impl LakeletCatalogProvider for IcebergRestCatalog {
    async fn list_schema_names(&self) -> Result<Vec<String>> {
        let mut names: Vec<_> = self
            .client()
            .await?
            .list_namespaces(None)
            .await
            .map_err(to_datafusion_error)?
            .into_iter()
            .map(|namespace| namespace.to_string())
            .collect();
        names.sort();
        Ok(names)
    }

    async fn list_table_names(&self, schema_name: &str) -> Result<Vec<String>> {
        Ok(self
            .client()
            .await?
            .list_tables(&namespace_from_schema(schema_name)?)
            .await
            .map_err(to_datafusion_error)?
            .into_iter()
            .map(|table| table.name)
            .collect())
    }

    async fn schema_exist(&self, schema_name: &str) -> Result<bool> {
        self.client()
            .await?
            .namespace_exists(&namespace_from_schema(schema_name)?)
            .await
            .map_err(to_datafusion_error)
    }

    async fn table_exist(&self, table_name: &str, schema_name: &str) -> Result<bool> {
        let client = self.client().await?;
        let ns = namespace_from_schema(schema_name)?;
        client
            .table_exists(&TableIdent::new(ns, table_name.into()))
            .await
            .map_err(to_datafusion_error)
    }
}

#[async_trait]
impl AsyncCatalogProvider for IcebergRestCatalog {
    async fn schema(&self, name: &str) -> Result<Option<Arc<dyn AsyncSchemaProvider>>> {
        let client = self.client().await?;
        let namespace = namespace_from_schema(name)?;
        if !client
            .namespace_exists(&namespace)
            .await
            .map_err(to_datafusion_error)?
        {
            return Ok(None);
        }
        Ok(Some(Arc::new(IcebergRestSchema {
            catalog_name: self.config.name.clone(),
            namespace,
            client,
        })))
    }
}

struct IcebergRestSchema {
    catalog_name: String,
    namespace: NamespaceIdent,
    client: Arc<dyn Catalog>,
}

#[async_trait]
impl AsyncSchemaProvider for IcebergRestSchema {
    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        let (base_name, metadata_type) = parse_table_reference(name)?;
        let ident = TableIdent::new(self.namespace.clone(), base_name);
        let table = match self.client.load_table(&ident).await {
            Ok(table) => table,
            Err(error)
                if matches!(
                    error.kind(),
                    ErrorKind::TableNotFound | ErrorKind::NamespaceNotFound
                ) =>
            {
                return Ok(None);
            }
            Err(error) => return Err(to_datafusion_error(error)),
        };
        if let Some(metadata_type) = metadata_type {
            return Ok(Some(Arc::new(IcebergMetadataTableProvider::try_new(
                table,
                metadata_type,
            )?)));
        }
        let reference = TableReference::full(
            self.catalog_name.as_str(),
            self.namespace.to_string(),
            ident.name.as_str(),
        );
        let location = table.metadata().location().to_string();
        Ok(Some(Arc::new(
            IcebergTableProvider::try_new_from_table(reference, location, table).await?,
        )))
    }
}
