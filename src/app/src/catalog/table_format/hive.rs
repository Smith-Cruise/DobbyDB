pub mod hive_partition;
pub mod hive_storage_info;
mod hive_table_provider;
mod hive_type;

use crate::table_format::hive::hive_partition::HivePartition;
use crate::table_format::hive::hive_storage_info::HiveStorageInfo;
use crate::table_format::hive::hive_table_provider::HiveTableProvider;
use datafusion::catalog::TableProvider;
use datafusion::common::Result;
use dobbydb_storage::storage::Storage;
use std::sync::Arc;

pub struct HiveTableProviderFactory {}

impl HiveTableProviderFactory {
    pub fn try_create_table_provider(
        info: HiveStorageInfo,
        partitions: Vec<HivePartition>,
        storage: Option<Storage>,
    ) -> Result<Arc<dyn TableProvider>> {
        let provider = HiveTableProvider::new(info, partitions, storage);
        Ok(Arc::new(provider))
    }
}
