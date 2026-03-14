pub mod hive_partition;
pub mod hive_storage_info;
mod hive_table_provider;
mod hive_type;

use crate::storage::StorageCredential;
use crate::table_format::hive::hive_partition::HivePartition;
use crate::table_format::hive::hive_storage_info::HiveStorageInfo;
use crate::table_format::hive::hive_table_provider::HiveTableProvider;
use datafusion::catalog::TableProvider;
use datafusion::common::Result;
use std::sync::Arc;
// #[derive(Debug, Clone, PartialEq)]
// pub enum HiveInputFormat {
//     TextFile,
//     Parquet,
// }
//
// #[derive(Debug, Clone)]
// pub struct HiveStorageInfo {
//     pub location: String,
//     pub input_format: HiveInputFormat,
//     pub data_cols: Vec<(String, String)>,
//     pub partition_cols: Vec<(String, String)>,
//     pub serde_properties: HashMap<String, String>,
//     pub storage_credential: Option<StorageCredential>,
// }

pub struct HiveTableProviderFactory {}

impl HiveTableProviderFactory {
    // pub fn detect_input_format(input_format: &str) -> Result<HiveInputFormat> {
    //     if input_format.contains("TextInputFormat") {
    //         Ok(HiveInputFormat::TextFile)
    //     } else if input_format.to_lowercase().contains("parquet") {
    //         Ok(HiveInputFormat::Parquet)
    //     } else {
    //         Err(DataFusionError::NotImplemented(format!(
    //             "unsupported Hive input format: {}",
    //             input_format
    //         )))
    //     }
    // }

    pub fn try_create_table_provider(
        info: HiveStorageInfo,
        partitions: Vec<HivePartition>,
        storage_credential: Option<StorageCredential>,
    ) -> Result<Arc<dyn TableProvider>> {
        let provider = HiveTableProvider::new(info, partitions, storage_credential);
        Ok(Arc::new(provider))
    }
}
