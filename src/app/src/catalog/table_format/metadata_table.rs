#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataTableType {
    FilePath,
    Snapshots,
    Manifests,
}

impl TryFrom<&str> for MetadataTableType {
    type Error = String;

    fn try_from(metadata_table_name: &str) -> Result<Self, Self::Error> {
        match metadata_table_name {
            "file_path" => Ok(MetadataTableType::FilePath),
            "snapshots" => Ok(MetadataTableType::Snapshots),
            "manifests" => Ok(MetadataTableType::Manifests),
            _ => Err(format!(
                "invalid metadata table type: {metadata_table_name}"
            )),
        }
    }
}
