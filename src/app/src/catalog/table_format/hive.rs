mod hive_file_utils;
mod hive_metadata_table_provider;
pub mod hive_partition;
pub mod hive_storage_info;
mod hive_table_provider;
mod hive_type;

use crate::table_format::hive::hive_metadata_table_provider::{
    HiveDataFilesMetadataTableProvider, HivePartitionsMetadataTableProvider,
};
use crate::table_format::hive::hive_partition::HivePartition;
use crate::table_format::hive::hive_storage_info::HiveStorageInfo;
use crate::table_format::hive::hive_table_provider::HiveTableProvider;
use crate::table_format::metadata_table::MetadataTableType;
use datafusion::catalog::TableProvider;
use datafusion::common::{DataFusionError, Result};
use dobbydb_storage::storage::Storage;
use std::sync::Arc;
use tokio::runtime::Handle;

pub struct HiveTableProviderFactory {}

impl HiveTableProviderFactory {
    pub fn try_create_table_provider(
        table_location: String,
        info: HiveStorageInfo,
        partitions: Vec<HivePartition>,
        metadata_table_type: Option<MetadataTableType>,
        storage: Option<Storage>,
        io_handle: Handle,
        table_definition: String,
    ) -> Result<Arc<dyn TableProvider>> {
        match metadata_table_type {
            Some(MetadataTableType::DataFiles) => {
                let provider =
                    HiveDataFilesMetadataTableProvider::new(table_location, partitions, storage);
                Ok(Arc::new(provider))
            }
            Some(MetadataTableType::Partitions) => {
                let partition_fields = info.table_schema.table_partition_cols().to_vec();
                let provider = HivePartitionsMetadataTableProvider::new(
                    table_location,
                    partition_fields,
                    partitions,
                    storage,
                );
                Ok(Arc::new(provider))
            }
            Some(metadata_table_type) => Err(DataFusionError::NotImplemented(format!(
                "hive metadata table {:?} is not supported",
                metadata_table_type
            ))),
            None => {
                let provider = HiveTableProvider::new(
                    table_location,
                    info,
                    partitions,
                    storage,
                    io_handle,
                    table_definition,
                );
                Ok(Arc::new(provider))
            }
        }
    }
}
