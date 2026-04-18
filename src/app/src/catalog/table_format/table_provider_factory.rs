use crate::catalog::CatalogConfig;
use crate::table_format::TableFormat;
use crate::table_format::delta::DeltaTableProviderFactory;
use crate::table_format::hive::HiveTableProviderFactory;
use crate::table_format::hive::hive_partition::HivePartition;
use crate::table_format::hive::hive_storage_info::HiveStorageInfo;
use crate::table_format::iceberg::IcebergTableProviderFactory;
use datafusion::catalog::TableProvider;
use datafusion::common::Result;
use datafusion::error::DataFusionError;
use datafusion::sql::TableReference;
use dobbydb_storage::storage::Storage;
use std::collections::HashMap;
use std::sync::Arc;

pub struct TableProviderBuilder {
    table_reference: TableReference,
    table_properties: HashMap<String, String>,
    table_format: TableFormat,
    metadata_table_name: Option<String>,
    hive_storage_info: Option<HiveStorageInfo>,
    hive_partitions: Option<Vec<HivePartition>>,
    storage: Option<Storage>,
}

impl TableProviderBuilder {
    pub fn new(
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
            table_reference,
            table_properties,
            table_format,
            metadata_table_name: None,
            hive_storage_info: None,
            hive_partitions: None,
            storage,
        }
    }

    pub fn with_table_metadata_table_name(mut self, metadata_table_name: Option<String>) -> Self {
        self.metadata_table_name = metadata_table_name;
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
                    self.metadata_table_name,
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
                    HiveTableProviderFactory::try_create_table_provider(
                        storage_info,
                        partitions,
                        self.storage,
                    )
                }
                _ => Err(DataFusionError::Internal(
                    "hive_storage_info or hive_partitions not existed".into(),
                )),
            },
        }
    }
}

pub fn split_table_name(tbl_name: &str) -> (&str, Option<&str>) {
    match tbl_name.split_once("$") {
        Some((tmp_table_name, tmp_metadata_table_name)) => {
            (tmp_table_name, Some(tmp_metadata_table_name))
        }
        None => (tbl_name, None),
    }
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
}
