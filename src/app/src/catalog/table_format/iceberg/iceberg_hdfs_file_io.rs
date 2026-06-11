use async_trait::async_trait;
use bytes::Bytes;
use datafusion::object_store::path::Path;
use datafusion::object_store::{Error as ObjectStoreError, ObjectStore, ObjectStoreExt};
use futures::stream::BoxStream;
use hdfs_native_object_store::{HdfsObjectStore, HdfsObjectStoreBuilder};
use iceberg::io::{
    FileMetadata, FileRead, FileWrite, InputFile, OutputFile, Storage, StorageConfig,
    StorageFactory,
};
use iceberg::{Error, ErrorKind, Result};
use serde::{Deserialize, Serialize};
use std::ops::Range;
use std::sync::{Arc, OnceLock};
use url::Url;

const HDFS_SCHEME: &str = "hdfs";

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct HdfsStorageFactory {
    authority: String,
}

impl HdfsStorageFactory {
    pub(crate) fn try_new(location: &str) -> Result<Self> {
        let parsed = parse_hdfs_url(location)?;
        Ok(Self {
            authority: parsed.authority().to_string(),
        })
    }
}

#[typetag::serde]
impl StorageFactory for HdfsStorageFactory {
    fn build(&self, _config: &StorageConfig) -> Result<Arc<dyn Storage>> {
        Ok(Arc::new(HdfsStorage::new(self.authority.clone())))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct HdfsStorage {
    authority: String,
    #[serde(skip)]
    store: Arc<OnceLock<Arc<HdfsObjectStore>>>,
}

impl HdfsStorage {
    fn new(authority: String) -> Self {
        Self {
            authority,
            store: Arc::new(OnceLock::new()),
        }
    }

    fn store(&self) -> Result<Arc<HdfsObjectStore>> {
        if let Some(store) = self.store.get() {
            return Ok(Arc::clone(store));
        }

        let store = Arc::new(
            HdfsObjectStoreBuilder::new()
                .with_url(format!("{HDFS_SCHEME}://{}", self.authority))
                .build()
                .map_err(|error| {
                    Error::new(ErrorKind::Unexpected, "Failed to build HDFS object store")
                        .with_source(error)
                })?,
        );
        let _ = self.store.set(Arc::clone(&store));
        Ok(Arc::clone(self.store.get().unwrap()))
    }

    fn get_relative_path(&self, location: &str) -> Result<Path> {
        let parsed = parse_hdfs_url(location)?;
        if parsed.authority() != self.authority {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!(
                    "HDFS authority mismatch: expected {}, got {}",
                    self.authority,
                    parsed.authority()
                ),
            ));
        }

        Path::from_url_path(parsed.path().trim_start_matches('/')).map_err(|error| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("Invalid HDFS path: {location}"),
            )
            .with_source(error)
        })
    }
}

#[async_trait]
#[typetag::serde]
impl Storage for HdfsStorage {
    async fn exists(&self, path: &str) -> Result<bool> {
        let path = self.get_relative_path(path)?;
        match self.store()?.head(&path).await {
            Ok(_) => Ok(true),
            Err(ObjectStoreError::NotFound { .. }) => Ok(false),
            Err(error) => Err(from_object_store_error("check HDFS file existence", error)),
        }
    }

    async fn metadata(&self, path: &str) -> Result<FileMetadata> {
        let path = self.get_relative_path(path)?;
        let metadata = self
            .store()?
            .head(&path)
            .await
            .map_err(|error| from_object_store_error("read HDFS file metadata", error))?;
        Ok(FileMetadata {
            size: metadata.size,
        })
    }

    async fn read(&self, path: &str) -> Result<Bytes> {
        let path = self.get_relative_path(path)?;
        self.store()?
            .get(&path)
            .await
            .map_err(|error| from_object_store_error("open HDFS file", error))?
            .bytes()
            .await
            .map_err(|error| from_object_store_error("read HDFS file", error))
    }

    async fn reader(&self, path: &str) -> Result<Box<dyn FileRead>> {
        Ok(Box::new(HdfsFileReader {
            store: self.store()?,
            path: self.get_relative_path(path)?,
        }))
    }

    async fn write(&self, _path: &str, _bs: Bytes) -> Result<()> {
        Err(read_only_error())
    }

    async fn writer(&self, _path: &str) -> Result<Box<dyn FileWrite>> {
        Err(read_only_error())
    }

    async fn delete(&self, _path: &str) -> Result<()> {
        Err(read_only_error())
    }

    async fn delete_prefix(&self, _path: &str) -> Result<()> {
        Err(read_only_error())
    }

    async fn delete_stream(&self, _paths: BoxStream<'static, String>) -> Result<()> {
        Err(read_only_error())
    }

    fn new_input(&self, path: &str) -> Result<InputFile> {
        self.get_relative_path(path)?;
        Ok(InputFile::new(Arc::new(self.clone()), path.to_string()))
    }

    fn new_output(&self, _path: &str) -> Result<OutputFile> {
        Err(read_only_error())
    }
}

#[derive(Debug)]
struct HdfsFileReader {
    store: Arc<dyn ObjectStore>,
    path: Path,
}

#[async_trait]
impl FileRead for HdfsFileReader {
    async fn read(&self, range: Range<u64>) -> Result<Bytes> {
        self.store
            .get_range(&self.path, range)
            .await
            .map_err(|error| from_object_store_error("read HDFS file range", error))
    }
}

fn parse_hdfs_url(location: &str) -> Result<Url> {
    let parsed = Url::parse(location).map_err(|error| {
        Error::new(
            ErrorKind::DataInvalid,
            format!("Invalid HDFS location: {location}"),
        )
        .with_source(error)
    })?;

    if parsed.scheme() != HDFS_SCHEME {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            format!("Invalid HDFS scheme: {}", parsed.scheme()),
        ));
    }
    if parsed.authority().is_empty() {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            format!("HDFS location has no authority: {location}"),
        ));
    }

    Ok(parsed)
}

fn from_object_store_error(operation: &str, error: ObjectStoreError) -> Error {
    Error::new(ErrorKind::Unexpected, format!("Failed to {operation}")).with_source(error)
}

fn read_only_error() -> Error {
    Error::new(
        ErrorKind::FeatureUnsupported,
        "HDFS Iceberg FileIO is read-only",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hdfs_factory_preserves_authority_port() {
        let factory =
            HdfsStorageFactory::try_new("hdfs://namenode:8020/warehouse/db/table/metadata.json")
                .unwrap();
        assert_eq!(factory.authority, "namenode:8020");
    }

    #[test]
    fn test_relative_path() {
        let storage = HdfsStorage::new("namenode:8020".to_string());

        assert_eq!(
            storage
                .get_relative_path("hdfs://namenode:8020/warehouse/db/table/metadata.json")
                .unwrap()
                .as_ref(),
            "warehouse/db/table/metadata.json"
        );
    }

    #[test]
    fn test_relative_path_decodes_url_encoding() {
        let storage = HdfsStorage::new("namenode:8020".to_string());

        assert_eq!(
            storage
                .get_relative_path("hdfs://namenode:8020/warehouse/table%20name/metadata.json")
                .unwrap()
                .as_ref(),
            "warehouse/table name/metadata.json"
        );
    }

    #[test]
    fn test_rejects_different_authority() {
        let storage = HdfsStorage::new("namenode:8020".to_string());

        let error = storage
            .get_relative_path("hdfs://other:8020/warehouse/metadata.json")
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::DataInvalid);
    }

    #[test]
    fn test_rejects_non_hdfs_location() {
        let error = HdfsStorageFactory::try_new("s3://bucket/metadata.json").unwrap_err();
        assert_eq!(error.kind(), ErrorKind::DataInvalid);
    }

    #[tokio::test]
    async fn test_write_operations_are_unsupported() {
        let storage = HdfsStorage::new("namenode:8020".to_string());

        let error = storage
            .write("hdfs://namenode:8020/warehouse/metadata.json", Bytes::new())
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::FeatureUnsupported);
        assert_eq!(
            storage
                .new_output("hdfs://namenode:8020/warehouse/metadata.json")
                .unwrap_err()
                .kind(),
            ErrorKind::FeatureUnsupported
        );
    }
}
