#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataTableType {
    DataFiles,
    Snapshots,
    Manifests,
}

impl TryFrom<&str> for MetadataTableType {
    type Error = String;

    fn try_from(metadata_table_name: &str) -> Result<Self, Self::Error> {
        match metadata_table_name {
            "data_files" => Ok(MetadataTableType::DataFiles),
            "snapshots" => Ok(MetadataTableType::Snapshots),
            "manifests" => Ok(MetadataTableType::Manifests),
            _ => Err(format!(
                "invalid metadata table type: {metadata_table_name}"
            )),
        }
    }
}
