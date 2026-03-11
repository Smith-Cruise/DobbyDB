use datafusion::common::Result;
use datafusion::error::DataFusionError;
use hive_metastore::Partition;

#[derive(Debug)]
pub struct HivePartition {
    pub location: String,              // partition location
    pub partition_values: Vec<String>, // same size as partition_keys
}

impl HivePartition {
    pub fn try_new_from_hms_partition(partition: &Partition) -> Result<Self> {
        let sd = match &partition.sd {
            Some(sd) => sd,
            None => {
                return Err(DataFusionError::Internal(String::from(
                    "partition sd not existed",
                )));
            }
        };
        let location = match &sd.location {
            Some(location) => location.to_string(),
            None => {
                return Err(DataFusionError::Internal(String::from(
                    "partition sd location not existed",
                )));
            }
        };
        let partition_values = match &partition.values {
            Some(partition) => partition.iter().map(|v| v.to_string()).collect(),
            None => {
                return Err(DataFusionError::Internal(String::from(
                    "partition values existed",
                )));
            }
        };
        Ok(HivePartition {
            location,
            partition_values,
        })
    }
}
