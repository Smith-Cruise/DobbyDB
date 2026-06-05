mod hive_file_utils;
mod hive_metadata_table_provider;
pub mod hive_partition;
pub mod hive_storage_info;
mod hive_table_provider;
mod hive_type;

use crate::table_format::hive::hive_metadata_table_provider::HiveMetadataTableProvider;
use crate::table_format::hive::hive_partition::HivePartition;
use crate::table_format::hive::hive_storage_info::HiveStorageInfo;
use crate::table_format::hive::hive_table_provider::HiveTableProvider;
use crate::table_format::metadata_table::MetadataTableType;
use datafusion::catalog::TableProvider;
use datafusion::common::Result;
use dobbydb_storage::storage::Storage;
use std::sync::Arc;
use tokio::runtime::Handle;

pub struct HiveTableProviderFactory {}

impl HiveTableProviderFactory {
    pub fn try_create_table_provider(
        info: HiveStorageInfo,
        partitions: Vec<HivePartition>,
        metadata_table_type: Option<MetadataTableType>,
        storage: Option<Storage>,
        io_handle: Handle,
        table_definition: String,
    ) -> Result<Arc<dyn TableProvider>> {
        match metadata_table_type {
            Some(metadata_table_type) => {
                let provider = HiveMetadataTableProvider::try_new(
                    info.table_location,
                    partitions,
                    metadata_table_type,
                    storage,
                )?;
                Ok(Arc::new(provider))
            }
            None => {
                let provider =
                    HiveTableProvider::new(info, partitions, storage, io_handle, table_definition);
                Ok(Arc::new(provider))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table_format::hive::hive_storage_info::HiveInputFormat;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::table_schema::TableSchema;
    use std::collections::HashMap;

    #[tokio::test]
    async fn test_hive_provider_returns_table_definition() -> Result<()> {
        let storage_info = HiveStorageInfo {
            table_location: "s3://bucket/path".to_string(),
            input_format: HiveInputFormat::Parquet,
            table_schema: TableSchema::new(
                Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)])),
                vec![],
            ),
            serde_properties: HashMap::new(),
            table_properties: HashMap::new(),
        };
        let provider = HiveTableProviderFactory::try_create_table_provider(
            storage_info,
            vec![],
            None,
            None,
            Handle::current(),
            "CREATE TABLE t".to_string(),
        )?;

        assert_eq!(provider.get_table_definition(), Some("CREATE TABLE t"));
        Ok(())
    }
}
