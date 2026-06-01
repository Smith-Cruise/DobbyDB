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
                let provider = HiveTableProvider::new(info, partitions, storage, io_handle);
                Ok(Arc::new(provider))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::Schema;
    use datafusion::datasource::table_schema::TableSchema;
    use std::collections::HashMap;

    #[tokio::test]
    async fn test_create_file_path_metadata_provider() {
        let provider = HiveTableProviderFactory::try_create_table_provider(
            build_test_storage_info(),
            vec![],
            Some(MetadataTableType::FilePath),
            None,
            Handle::current(),
        )
        .unwrap();

        assert_eq!(provider.schema().fields().len(), 2);
        assert!(provider.schema().field_with_name("file_path").is_ok());
        assert!(provider.schema().field_with_name("file_size").is_ok());
    }

    fn build_test_storage_info() -> HiveStorageInfo {
        HiveStorageInfo {
            table_location: "s3://warehouse/hive/table".to_string(),
            input_format: hive_storage_info::HiveInputFormat::Parquet,
            table_schema: TableSchema::new(Arc::new(Schema::empty()), vec![]),
            serde_properties: HashMap::new(),
            table_properties: HashMap::new(),
        }
    }
}
