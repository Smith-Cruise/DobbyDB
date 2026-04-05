use aws_sdk_glue::types::Partition as GluePartition;
use datafusion::common::Result;
use datafusion::error::DataFusionError;
use hive_metastore::Partition as HMSPartition;

#[derive(Debug)]
pub struct HivePartition {
    pub location: String,              // partition location
    pub partition_values: Vec<String>, // same size as partition_keys
}

impl HivePartition {
    pub fn try_new_from_hms_partition(partition: &HMSPartition) -> Result<Self> {
        let sd = partition
            .sd
            .as_ref()
            .ok_or_else(|| DataFusionError::Internal("partition sd not existed".to_string()))?;
        Self::try_new(
            sd.location.as_deref(),
            partition
                .values
                .as_ref()
                .map(|partition| partition.iter().map(|v| v.to_string()).collect()),
        )
    }

    pub fn try_new_from_glue_partition(partition: &GluePartition) -> Result<Self> {
        let sd = partition
            .storage_descriptor()
            .ok_or_else(|| DataFusionError::Internal("partition sd not existed".to_string()))?;
        Self::try_new(sd.location(), partition.values.clone())
    }

    fn try_new(location: Option<&str>, partition_values: Option<Vec<String>>) -> Result<Self> {
        let location = location.map(ToString::to_string).ok_or_else(|| {
            DataFusionError::Internal("partition sd location not existed".to_string())
        })?;
        let partition_values = partition_values
            .ok_or_else(|| DataFusionError::Internal("partition values not existed".to_string()))?;

        Ok(HivePartition {
            location,
            partition_values,
        })
    }
}
