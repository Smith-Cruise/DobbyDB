use crate::context::DobbyDbContext;
use crate::glue_catalog::{GlueCatalog, GlueCatalogConfig};
use crate::hms_catalog::{HMSCatalog, HMSCatalogConfig};
use crate::internal_catalog::{INTERNAL_CATALOG, InternalCatalog};
use crate::table_format::TableFormat;
use async_trait::async_trait;
use datafusion::catalog::{AsyncCatalogProvider, AsyncCatalogProviderList};
use datafusion::common::Result;
use datafusion::error::DataFusionError;
use dobbydb_common::runtime::RuntimeManager;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Default, Serialize, Deserialize)]
pub struct CatalogConfigs {
    pub hms: Option<Vec<HMSCatalogConfig>>,
    pub glue: Option<Vec<GlueCatalogConfig>>,
}

#[allow(clippy::upper_case_acronyms)]
#[derive(Debug, Clone)]
pub enum CatalogConfig {
    Internal,
    HMS(HMSCatalogConfig),
    GLUE(GlueCatalogConfig),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShowCreateTable {
    pub table_catalog: String,
    pub table_schema: String,
    pub table_name: String,
    pub definition: String,
}

impl ShowCreateTable {
    pub fn try_new(
        table_catalog: impl Into<String>,
        table_schema: impl Into<String>,
        table_name: impl Into<String>,
        table_format: TableFormat,
        partition_columns: Vec<String>,
        location: impl Into<String>,
    ) -> Result<Self> {
        let table_catalog = table_catalog.into();
        let table_schema = table_schema.into();
        let table_name = table_name.into();
        let definition = build_show_create_table_definition(
            &table_catalog,
            &table_schema,
            &table_name,
            table_format,
            &partition_columns,
            &location.into(),
        )?;

        Ok(Self {
            table_catalog,
            table_schema,
            table_name,
            definition,
        })
    }
}

fn build_show_create_table_definition(
    table_catalog: &str,
    table_schema: &str,
    table_name: &str,
    table_format: TableFormat,
    partition_columns: &[String],
    location: &str,
) -> Result<String> {
    let using = match table_format {
        TableFormat::Hive => "hive",
        TableFormat::Iceberg => {
            return Err(DataFusionError::NotImplemented(
                "SHOW CREATE TABLE is not implemented for Iceberg tables".to_string(),
            ));
        }
        TableFormat::Delta => {
            return Err(DataFusionError::NotImplemented(
                "SHOW CREATE TABLE is not implemented for Delta tables".to_string(),
            ));
        }
    };

    let mut definition = format!(
        "CREATE TABLE `{}`.`{}`.`{}`\nUSING {}\n",
        escape_backtick_identifier(table_catalog),
        escape_backtick_identifier(table_schema),
        escape_backtick_identifier(table_name),
        using
    );
    if !partition_columns.is_empty() {
        let partition_columns = partition_columns
            .iter()
            .map(|name| format!("`{}`", escape_backtick_identifier(name)))
            .collect::<Vec<_>>()
            .join(", ");
        definition.push_str(&format!("PARTITIONED BY ({partition_columns})\n"));
    }
    definition.push_str(&format!("LOCATION '{}'", location.replace('\'', "\\'")));
    Ok(definition)
}

fn escape_backtick_identifier(value: &str) -> String {
    value.replace('`', "``")
}

#[derive(Debug, Clone)]
pub struct CatalogManager {
    catalogs: HashMap<String, CatalogConfig>,
}

impl Default for CatalogManager {
    fn default() -> Self {
        Self::new()
    }
}

impl CatalogManager {
    pub fn new() -> Self {
        let mut catalogs = HashMap::new();
        catalogs.insert(INTERNAL_CATALOG.to_string(), CatalogConfig::Internal);
        Self { catalogs }
    }
    fn add_catalog(&mut self, catalog_name: &str, catalog_config: CatalogConfig) -> Result<()> {
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

    pub fn load_catalogs(&mut self, catalogs: &CatalogConfigs) -> Result<()> {
        if let Some(ref hms_catalogs) = catalogs.hms {
            for hms_catalog in hms_catalogs {
                self.add_catalog(&hms_catalog.name, CatalogConfig::HMS(hms_catalog.clone()))?;
            }
        }

        if let Some(ref glue_catalogs) = catalogs.glue {
            for glue_catalog in glue_catalogs {
                self.add_catalog(
                    &glue_catalog.name,
                    CatalogConfig::GLUE(glue_catalog.clone()),
                )?;
            }
        }

        Ok(())
    }

    pub fn get_catalog(&self, catalog_name: &str) -> Option<&CatalogConfig> {
        self.catalogs.get(catalog_name)
    }

    pub fn list_catalogs(&self) -> Vec<(String, CatalogConfig)> {
        self.catalogs
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    fn build_catalog_provider(
        &self,
        catalog_name: &str,
    ) -> Result<Box<dyn DobbyDbCatalogProvider + Send + Sync>> {
        let catalog_config = self
            .get_catalog(catalog_name)
            .ok_or_else(|| DataFusionError::Plan(format!("unknown catalog {}", catalog_name)))?;
        let dobbydb_context = Arc::new(DobbyDbContext {
            server_config: Default::default(),
            catalog_manager: Arc::new(self.clone()),
            runtime_manager: Arc::new(RuntimeManager::default()),
            default_catalog: None,
            default_schema: None,
        });

        match catalog_config {
            CatalogConfig::Internal => Ok(Box::new(InternalCatalog::new(dobbydb_context))),
            CatalogConfig::HMS(hms_catalog) => Ok(Box::new(HMSCatalog::new(
                dobbydb_context,
                Arc::new(hms_catalog.clone()),
            ))),
            CatalogConfig::GLUE(glue_catalog) => Ok(Box::new(GlueCatalog::new(
                dobbydb_context,
                Arc::new(glue_catalog.clone()),
            ))),
        }
    }

    pub fn catalog_exists(&self, catalog_name: &str) -> bool {
        self.catalogs.contains_key(catalog_name)
    }

    pub async fn list_schema_names(&self, catalog_name: &str) -> Result<Vec<String>> {
        self.build_catalog_provider(catalog_name)?
            .list_schema_names()
            .await
    }

    pub async fn list_table_names(
        &self,
        catalog_name: &str,
        schema_name: &str,
    ) -> Result<Vec<String>> {
        self.build_catalog_provider(catalog_name)?
            .list_table_names(schema_name)
            .await
    }

    pub async fn schema_exist(&self, catalog_name: &str, schema_name: &str) -> Result<bool> {
        self.build_catalog_provider(catalog_name)?
            .schema_exist(schema_name)
            .await
    }

    pub async fn table_exists(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<bool> {
        self.build_catalog_provider(catalog_name)?
            .table_exist(table_name, schema_name)
            .await
    }

    pub async fn show_create_table(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<ShowCreateTable> {
        self.build_catalog_provider(catalog_name)?
            .show_create_table(table_name, schema_name)
            .await
    }
}

pub struct DobbyDbCatalogProviderList {
    dobbydb_context: Arc<DobbyDbContext>,
}

impl DobbyDbCatalogProviderList {
    pub fn new(dobbydb_context: Arc<DobbyDbContext>) -> DobbyDbCatalogProviderList {
        Self { dobbydb_context }
    }
}

#[async_trait]
impl AsyncCatalogProviderList for DobbyDbCatalogProviderList {
    async fn catalog(&self, catalog_name: &str) -> Result<Option<Arc<dyn AsyncCatalogProvider>>> {
        let catalog_config = if let Some(catalog_config) = self
            .dobbydb_context
            .catalog_manager
            .get_catalog(catalog_name)
        {
            catalog_config.clone()
        } else {
            return Ok(None);
        };

        match catalog_config {
            CatalogConfig::Internal => Ok(Some(Arc::new(
                crate::internal_catalog::InternalCatalog::new(self.dobbydb_context.clone()),
            ))),
            CatalogConfig::HMS(hms_catalog) => Ok(Some(Arc::new(HMSCatalog::new(
                self.dobbydb_context.clone(),
                Arc::new(hms_catalog),
            )))),
            CatalogConfig::GLUE(glue_catalog) => Ok(Some(Arc::new(GlueCatalog::new(
                self.dobbydb_context.clone(),
                Arc::new(glue_catalog),
            )))),
        }
    }
}

#[async_trait]
pub trait DobbyDbCatalogProvider {
    async fn list_schema_names(&self) -> Result<Vec<String>>;

    async fn list_table_names(&self, schema_name: &str) -> Result<Vec<String>>;

    async fn schema_exist(&self, schema_name: &str) -> Result<bool>;

    async fn table_exist(&self, table_name: &str, schema_name: &str) -> Result<bool>;

    async fn show_create_table(
        &self,
        table_name: &str,
        schema_name: &str,
    ) -> Result<ShowCreateTable>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_hive_show_create_table_definition_with_partitions() -> Result<()> {
        let definition = build_show_create_table_definition(
            "catalog",
            "schema",
            "table",
            TableFormat::Hive,
            &["dt".to_string(), "region".to_string()],
            "s3://bucket/path",
        )?;

        assert_eq!(
            definition,
            "CREATE TABLE `catalog`.`schema`.`table`\nUSING hive\nPARTITIONED BY (`dt`, `region`)\nLOCATION 's3://bucket/path'"
        );
        Ok(())
    }

    #[test]
    fn test_build_hive_show_create_table_definition_without_partitions() -> Result<()> {
        let definition = build_show_create_table_definition(
            "catalog",
            "schema",
            "table",
            TableFormat::Hive,
            &[],
            "s3://bucket/path",
        )?;

        assert_eq!(
            definition,
            "CREATE TABLE `catalog`.`schema`.`table`\nUSING hive\nLOCATION 's3://bucket/path'"
        );
        Ok(())
    }

    #[test]
    fn test_build_show_create_table_definition_rejects_iceberg() {
        let err = build_show_create_table_definition(
            "catalog",
            "schema",
            "table",
            TableFormat::Iceberg,
            &[],
            "s3://bucket/path",
        )
        .unwrap_err();

        assert!(err.to_string().contains("Iceberg tables"));
    }

    #[test]
    fn test_build_show_create_table_definition_rejects_delta() {
        let err = build_show_create_table_definition(
            "catalog",
            "schema",
            "table",
            TableFormat::Delta,
            &[],
            "s3://bucket/path",
        )
        .unwrap_err();

        assert!(err.to_string().contains("Delta tables"));
    }
}
