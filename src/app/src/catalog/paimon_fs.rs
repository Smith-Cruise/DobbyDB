use crate::catalog::{CatalogConfig, LakeletCatalogProvider};
use crate::context::LakeletContext;
use crate::table_format::TableFormat;
use crate::table_format::table_provider_factory::{TableProviderBuilder, parse_table_reference};
use async_trait::async_trait;
use datafusion::catalog::{AsyncCatalogProvider, AsyncSchemaProvider, TableProvider};
use datafusion::common::Result;
use datafusion::common::TableReference;
use datafusion::error::DataFusionError;
use lakelet_storage::storage::Storage;
use paimon::catalog::{Catalog, Identifier};
use paimon::{CatalogOptions, FileSystemCatalog, Options};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaimonFSCatalogConfig {
    pub name: String,
    pub warehouse: String,
    #[serde(flatten, default)]
    pub storage: Storage,
}

fn to_datafusion_error(error: paimon::Error) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

/// Paimon filesystem catalog layout: warehouse/<db>.db/<table>.
fn table_location(warehouse: &str, schema_name: &str, table_name: &str) -> String {
    format!(
        "{}/{}.db/{}",
        warehouse.trim_end_matches('/'),
        schema_name,
        table_name
    )
}

pub struct PaimonFSCatalog {
    lakelet_context: Arc<LakeletContext>,
    config: Arc<PaimonFSCatalogConfig>,
    inner_catalog: Arc<FileSystemCatalog>,
}

impl PaimonFSCatalog {
    pub fn try_new(
        lakelet_context: Arc<LakeletContext>,
        config: Arc<PaimonFSCatalogConfig>,
    ) -> Result<Self> {
        let mut options = Options::from_map(config.storage.build_paimon_file_io_properties());
        options.set(
            CatalogOptions::WAREHOUSE,
            config.warehouse.trim_end_matches('/'),
        );
        let inner_catalog = Arc::new(FileSystemCatalog::new(options).map_err(to_datafusion_error)?);
        Ok(Self {
            lakelet_context,
            config,
            inner_catalog,
        })
    }
}

#[async_trait]
impl LakeletCatalogProvider for PaimonFSCatalog {
    async fn list_schema_names(&self) -> Result<Vec<String>> {
        self.inner_catalog
            .list_databases()
            .await
            .map_err(to_datafusion_error)
    }

    async fn list_table_names(&self, schema_name: &str) -> Result<Vec<String>> {
        self.inner_catalog
            .list_tables(schema_name)
            .await
            .map_err(to_datafusion_error)
    }

    async fn schema_exist(&self, schema_name: &str) -> Result<bool> {
        match self.inner_catalog.get_database(schema_name).await {
            Ok(_) => Ok(true),
            Err(paimon::Error::DatabaseNotExist { .. }) => Ok(false),
            Err(error) => Err(to_datafusion_error(error)),
        }
    }

    async fn table_exist(&self, table_name: &str, schema_name: &str) -> Result<bool> {
        let identifier = Identifier::new(schema_name, table_name);
        match self.inner_catalog.get_table(&identifier).await {
            Ok(_) => Ok(true),
            Err(paimon::Error::TableNotExist { .. } | paimon::Error::DatabaseNotExist { .. }) => {
                Ok(false)
            }
            Err(error) => Err(to_datafusion_error(error)),
        }
    }
}

#[async_trait]
impl AsyncCatalogProvider for PaimonFSCatalog {
    async fn schema(&self, schema_name: &str) -> Result<Option<Arc<dyn AsyncSchemaProvider>>> {
        Ok(Some(Arc::new(PaimonFSSchema::new(
            self.lakelet_context.clone(),
            self.config.clone(),
            schema_name,
        ))))
    }
}

struct PaimonFSSchema {
    lakelet_context: Arc<LakeletContext>,
    config: Arc<PaimonFSCatalogConfig>,
    schema_name: String,
}

impl PaimonFSSchema {
    pub fn new(
        lakelet_context: Arc<LakeletContext>,
        config: Arc<PaimonFSCatalogConfig>,
        schema_name: &str,
    ) -> Self {
        Self {
            lakelet_context,
            config,
            schema_name: schema_name.to_string(),
        }
    }
}

#[async_trait]
impl AsyncSchemaProvider for PaimonFSSchema {
    async fn table(&self, tbl_name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        let (table_name, metadata_table_type) = parse_table_reference(tbl_name)?;

        let table_location = table_location(&self.config.warehouse, &self.schema_name, &table_name);
        let table_reference = TableReference::full(
            self.config.name.as_str(),
            self.schema_name.as_str(),
            table_name.as_str(),
        );

        // The table format is fixed by the catalog type, no deduction needed.
        let table_provider_builder = TableProviderBuilder::new(
            self.lakelet_context.clone(),
            table_location,
            table_reference,
            HashMap::new(),
            TableFormat::Paimon,
            CatalogConfig::PaimonFS(self.config.deref().clone()),
        )
        .with_metadata_table_type(metadata_table_type);
        Ok(Some(table_provider_builder.build().await?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_paimon_fs_config_without_storage_block() {
        let config: PaimonFSCatalogConfig = toml::from_str(
            r#"
            name = "paimon_fs_1"
            warehouse = "/tmp/warehouse"
        "#,
        )
        .unwrap();
        assert_eq!(config.name, "paimon_fs_1");
        assert_eq!(config.warehouse, "/tmp/warehouse");
        // No storage block: the object-store schemes fall back to OpenDAL's
        // credential chain (covered in lakelet_storage::storage).
        assert!(config.storage.s3_storage.is_none());
    }

    #[test]
    fn test_parse_paimon_fs_config_with_storage_block() {
        let config: PaimonFSCatalogConfig = toml::from_str(
            r#"
            name = "paimon_fs_1"
            warehouse = "s3://bucket/warehouse"
            s3-storage = { endpoint = "http://127.0.0.1:9000", region = "us-east-1", access-key = "ak", secret-key = "sk" }
        "#,
        )
        .unwrap();
        assert_eq!(config.warehouse, "s3://bucket/warehouse");
        assert!(config.storage.build_operator("s3", "bucket").is_ok());
    }

    #[test]
    fn test_parse_paimon_fs_config_requires_warehouse() {
        let result = toml::from_str::<PaimonFSCatalogConfig>(
            r#"
            name = "paimon_fs_1"
        "#,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_table_location() {
        assert_eq!(
            table_location("s3://bucket/warehouse", "db1", "t1"),
            "s3://bucket/warehouse/db1.db/t1"
        );
        assert_eq!(
            table_location("s3://bucket/warehouse/", "db1", "t1"),
            "s3://bucket/warehouse/db1.db/t1"
        );
    }

    #[tokio::test]
    async fn test_paimon_fs_catalog_lists_databases_and_tables() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let config = PaimonFSCatalogConfig {
            name: "paimon_fs_1".to_string(),
            warehouse: temp_dir.path().to_str().unwrap().to_string(),
            storage: Storage::default(),
        };

        let catalog =
            PaimonFSCatalog::try_new(Arc::new(LakeletContext::default()), Arc::new(config))
                .unwrap();

        // Provision a database and a table with the upstream filesystem catalog.
        let inner_catalog = catalog.inner_catalog.clone();
        inner_catalog
            .create_database("db1", false, HashMap::new())
            .await
            .unwrap();
        let schema = paimon::spec::Schema::builder()
            .column(
                "id",
                paimon::spec::DataType::Int(paimon::spec::IntType::new()),
            )
            .build()
            .unwrap();
        inner_catalog
            .create_table(&Identifier::new("db1", "t1"), schema, false)
            .await
            .unwrap();

        assert_eq!(catalog.list_schema_names().await.unwrap(), vec!["db1"]);
        assert_eq!(catalog.list_table_names("db1").await.unwrap(), vec!["t1"]);
        assert!(catalog.schema_exist("db1").await.unwrap());
        assert!(!catalog.schema_exist("db_missing").await.unwrap());
        assert!(catalog.table_exist("t1", "db1").await.unwrap());
        assert!(!catalog.table_exist("t_missing", "db1").await.unwrap());
        assert!(!catalog.table_exist("t1", "db_missing").await.unwrap());
    }

    #[tokio::test]
    async fn test_paimon_fs_metadata_tables() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let config = Arc::new(PaimonFSCatalogConfig {
            name: "paimon_fs_1".to_string(),
            warehouse: temp_dir.path().to_str().unwrap().to_string(),
            storage: Storage::default(),
        });
        let lakelet_context = Arc::new(LakeletContext::default());
        let catalog = PaimonFSCatalog::try_new(lakelet_context.clone(), config.clone()).unwrap();

        let inner_catalog = catalog.inner_catalog.clone();
        inner_catalog
            .create_database("db1", false, HashMap::new())
            .await
            .unwrap();
        let schema = paimon::spec::Schema::builder()
            .column(
                "id",
                paimon::spec::DataType::Int(paimon::spec::IntType::new()),
            )
            .build()
            .unwrap();
        inner_catalog
            .create_table(&Identifier::new("db1", "t1"), schema, false)
            .await
            .unwrap();

        let schema_provider = PaimonFSSchema::new(lakelet_context, config, "db1");
        let ctx = datafusion::prelude::SessionContext::new();

        // `t1$schemas` resolves through paimon-datafusion's system tables and
        // returns the single schema written by create_table.
        let provider = schema_provider.table("t1$schemas").await.unwrap().unwrap();
        let batches = ctx.read_table(provider).unwrap().collect().await.unwrap();
        let row_count: usize = batches.iter().map(|batch| batch.num_rows()).sum();
        assert_eq!(row_count, 1);

        // `t1$options` keeps the key/value shape even with no options set.
        let provider = schema_provider.table("t1$options").await.unwrap().unwrap();
        let field_names: Vec<_> = provider
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect();
        assert_eq!(field_names, vec!["key", "value"]);

        // `t1$snapshots` is empty before any write but must still scan.
        let provider = schema_provider
            .table("t1$snapshots")
            .await
            .unwrap()
            .unwrap();
        let batches = ctx.read_table(provider).unwrap().collect().await.unwrap();
        let row_count: usize = batches.iter().map(|batch| batch.num_rows()).sum();
        assert_eq!(row_count, 0);

        // `history` is an Iceberg-only metadata table.
        let error = schema_provider.table("t1$history").await.unwrap_err();
        assert!(
            matches!(error, DataFusionError::NotImplemented(_)),
            "{error:?}"
        );
    }
}
