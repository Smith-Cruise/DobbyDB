use crate::catalog::{CatalogConfig, TableDefinitionBuilder};
use crate::context::DobbyDbContext;
use crate::table_format::TableFormat;
use crate::table_format::delta::DeltaTableProviderFactory;
use crate::table_format::hive::HiveTableProviderFactory;
use crate::table_format::hive::hive_partition::HivePartition;
use crate::table_format::hive::hive_storage_info::HiveStorageInfo;
use crate::table_format::iceberg::IcebergTableProviderFactory;
use crate::table_format::metadata_table::MetadataTableType;
use datafusion::catalog::TableProvider;
use datafusion::common::Result;
use datafusion::error::DataFusionError;
use datafusion::sql::TableReference;
use dobbydb_storage::storage::Storage;
use std::collections::HashMap;
use std::sync::Arc;

pub struct TableProviderBuilder {
    dobbydb_context: Arc<DobbyDbContext>,
    table_reference: TableReference,
    table_properties: HashMap<String, String>,
    table_format: TableFormat,
    metadata_table_type: Option<MetadataTableType>,
    hive_storage_info: Option<HiveStorageInfo>,
    hive_partitions: Option<Vec<HivePartition>>,
    storage: Option<Storage>,
}

impl TableProviderBuilder {
    pub fn new(
        dobbydb_context: Arc<DobbyDbContext>,
        table_reference: TableReference,
        table_properties: HashMap<String, String>,
        table_format: TableFormat,
        catalog_config: CatalogConfig,
    ) -> Self {
        let storage = match catalog_config {
            CatalogConfig::GLUE(glue_config) => glue_config.storage.clone(),
            CatalogConfig::HMS(hms_config) => hms_config.storage.clone(),
            CatalogConfig::Internal => {
                panic!("unreachable")
            }
        };
        Self {
            dobbydb_context,
            table_reference,
            table_properties,
            table_format,
            metadata_table_type: None,
            hive_storage_info: None,
            hive_partitions: None,
            storage,
        }
    }

    pub fn with_metadata_table_type(
        mut self,
        metadata_table_type: Option<MetadataTableType>,
    ) -> Self {
        self.metadata_table_type = metadata_table_type;
        self
    }

    pub fn with_hive_storage_info(mut self, hive_storage_info: Option<HiveStorageInfo>) -> Self {
        self.hive_storage_info = hive_storage_info;
        self
    }

    pub fn with_hive_partitions(mut self, hive_partitions: Option<Vec<HivePartition>>) -> Self {
        self.hive_partitions = hive_partitions;
        self
    }

    #[allow(dead_code)]
    pub fn table_format(&self) -> &TableFormat {
        &self.table_format
    }

    pub async fn build(self) -> Result<Arc<dyn TableProvider>> {
        match self.table_format {
            TableFormat::Iceberg => {
                let iceberg_metadata_location =
                    self.table_properties.get("metadata_location").ok_or(
                        DataFusionError::Internal("metadata_location not existed".into()),
                    )?;
                IcebergTableProviderFactory::try_create_table_provider(
                    self.table_reference,
                    iceberg_metadata_location.clone(),
                    self.metadata_table_type,
                    self.storage,
                )
                .await
            }
            TableFormat::Delta => {
                let table_location = self
                    .table_properties
                    .get("location")
                    .ok_or(DataFusionError::Internal("location not existed".into()))?;
                DeltaTableProviderFactory::try_create_table_provider(
                    self.table_reference,
                    table_location.clone(),
                    self.storage,
                )
                .await
            }
            TableFormat::Hive => match (self.hive_storage_info, self.hive_partitions) {
                (Some(storage_info), Some(partitions)) => {
                    let io_handle = self.dobbydb_context.runtime_manager.io_handle();
                    let table_definition = TableDefinitionBuilder::new(
                        TableFormat::Hive,
                        self.table_reference.clone(),
                        storage_info.table_schema.table_schema().as_ref().clone(),
                        storage_info.table_location.clone(),
                    )
                    .with_partition_column_names(
                        storage_info
                            .table_schema
                            .table_partition_cols()
                            .iter()
                            .map(|field| field.name().to_string())
                            .collect(),
                    )
                    .build()?;
                    HiveTableProviderFactory::try_create_table_provider(
                        storage_info,
                        partitions,
                        self.metadata_table_type,
                        self.storage,
                        io_handle,
                        table_definition,
                    )
                }
                _ => Err(DataFusionError::Internal(
                    "hive_storage_info or hive_partitions not existed".into(),
                )),
            },
        }
    }
}

pub fn parse_table_reference(tbl_name: &str) -> Result<(String, Option<MetadataTableType>)> {
    let (table_name, metadata_table_name) = match tbl_name.split_once("$") {
        Some((table_name, metadata_table_name)) => (table_name, Some(metadata_table_name)),
        None => (tbl_name, None),
    };

    if table_name.is_empty() {
        return Err(DataFusionError::Plan("table name can't be empty".into()));
    }

    let metadata_table_type = metadata_table_name
        .map(MetadataTableType::try_from)
        .transpose()
        .map_err(DataFusionError::Plan)?;

    Ok((table_name.to_string(), metadata_table_type))
}

pub fn deduce_table_format(table_properties: &HashMap<String, String>) -> Result<TableFormat> {
    if table_properties.contains_key("metadata_location") {
        return Ok(TableFormat::Iceberg);
    }
    if let Some(spark_provider) = table_properties.get("spark.sql.sources.provider")
        && spark_provider == "DELTA"
    {
        return Ok(TableFormat::Delta);
    }

    // other table format fallback to hive format
    Ok(TableFormat::Hive)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table_format::hive::hive_storage_info::HiveInputFormat;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::table_schema::TableSchema;

    #[test]
    fn test_parse_table_reference() {
        assert_eq!(
            ("tbl".to_string(), None),
            parse_table_reference("tbl").unwrap()
        );
        assert_eq!(
            ("tbl".to_string(), Some(MetadataTableType::Snapshots)),
            parse_table_reference("tbl$snapshots").unwrap()
        );
        assert_eq!(
            ("tbl".to_string(), Some(MetadataTableType::Manifests)),
            parse_table_reference("tbl$manifests").unwrap()
        );
        assert_eq!(
            ("tbl".to_string(), Some(MetadataTableType::FilePath)),
            parse_table_reference("tbl$file_path").unwrap()
        );
        assert!(parse_table_reference("tbl$unknown").is_err());
        assert!(parse_table_reference("$snapshots").is_err());
    }

    #[test]
    fn test_deduce_table_format() {
        let table_properties =
            HashMap::from([("metadata_location".to_string(), "path".to_string())]);
        assert_eq!(
            TableFormat::Iceberg,
            deduce_table_format(&table_properties).unwrap()
        );
        let table_properties = HashMap::from([(
            "spark.sql.sources.provider".to_string(),
            "DELTA".to_string(),
        )]);
        assert_eq!(
            TableFormat::Delta,
            deduce_table_format(&table_properties).unwrap()
        );
        let table_properties = HashMap::from([]);
        assert_eq!(
            TableFormat::Hive,
            deduce_table_format(&table_properties).unwrap()
        );
    }

    #[tokio::test]
    async fn test_build_hive_provider_generates_table_definition() -> Result<()> {
        let provider = TableProviderBuilder::new(
            Arc::new(DobbyDbContext::default()),
            TableReference::full("catalog", "schema", "table"),
            HashMap::new(),
            TableFormat::Hive,
            CatalogConfig::HMS(crate::hms_catalog::HMSCatalogConfig {
                name: "catalog".to_string(),
                metastore_uri: "localhost:9083".to_string(),
                storage: None,
            }),
        )
        .with_hive_storage_info(Some(test_hive_storage_info()))
        .with_hive_partitions(Some(vec![]))
        .build()
        .await?;

        assert_eq!(
            provider.get_table_definition(),
            Some(
                "CREATE TABLE `catalog`.`schema`.`table`\n(\n  `id` Int64\n)\nUSING HIVE\nLOCATION 's3://bucket/path'"
            )
        );
        Ok(())
    }

    fn test_hive_storage_info() -> HiveStorageInfo {
        HiveStorageInfo {
            table_location: "s3://bucket/path".to_string(),
            input_format: HiveInputFormat::Parquet,
            table_schema: TableSchema::new(
                Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)])),
                vec![],
            ),
            serde_properties: HashMap::new(),
            table_properties: HashMap::new(),
        }
    }
}
