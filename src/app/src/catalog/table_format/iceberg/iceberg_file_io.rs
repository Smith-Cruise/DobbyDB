use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use futures::stream::BoxStream;
use iceberg::io::{
    FileMetadata, FileRead, FileWrite, InputFile, OSS_ACCESS_KEY_ID, OSS_ACCESS_KEY_SECRET,
    OSS_ENDPOINT, OutputFile, S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_PATH_STYLE_ACCESS, S3_REGION,
    S3_SECRET_ACCESS_KEY, S3_SESSION_TOKEN, Storage, StorageConfig, StorageFactory,
};
use iceberg::{Error, ErrorKind, Result};
use lakelet_storage::oss_storage::OSSStorage;
use lakelet_storage::s3_storage::S3Storage;
use lakelet_storage::storage;
use opendal::Operator;
use percent_encoding::percent_decode_str;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::ops::Range;
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct IcebergStorageFactory {
    storage: storage::Storage,
}

impl IcebergStorageFactory {
    pub(crate) fn new(storage: storage::Storage) -> Self {
        Self { storage }
    }
}

#[typetag::serde]
impl StorageFactory for IcebergStorageFactory {
    fn build(&self, config: &StorageConfig) -> Result<Arc<dyn Storage>> {
        // Only the REST catalog puts anything in these properties, so for
        // every other catalog this resolves to the config as given.
        let storage = resolve_credentials(&self.storage, config);
        Ok(Arc::new(LakeletIcebergStorage::new(storage)))
    }
}

/// Resolve each location scheme against the catalog's own storage config first,
/// falling back to the credentials the catalog vended for this table.
///
/// A configured block wins outright: it is taken as a deliberate choice of
/// credentials and endpoint, so a block naming only a region still suppresses
/// the vended keys for that scheme. Merging the two field by field would make
/// which endpoint a request reaches depend on the server's response.
///
/// A scheme counts as vended only when its key pair is complete: FileIO
/// properties are the catalog's response merged with our own catalog
/// properties, so they also carry the bearer token, the access-delegation
/// header and possibly a bare `s3.region`. A non-empty property map says
/// nothing on its own.
fn resolve_credentials(base: &storage::Storage, config: &StorageConfig) -> storage::Storage {
    let vended_s3 =
        pair(config, S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY).map(|(access_key, secret_key)| {
            S3Storage {
                region: prop(config, S3_REGION),
                endpoint: prop(config, S3_ENDPOINT),
                access_key: Some(access_key),
                secret_key: Some(secret_key),
                session_token: prop(config, S3_SESSION_TOKEN),
                path_style_access: path_style(config),
            }
        });
    let vended_oss =
        pair(config, OSS_ACCESS_KEY_ID, OSS_ACCESS_KEY_SECRET).map(|(access_key, secret_key)| {
            OSSStorage {
                endpoint: prop(config, OSS_ENDPOINT),
                access_key: Some(access_key),
                secret_key: Some(secret_key),
                path_style_access: false,
            }
        });

    // A scheme neither side supplies stays unset, so `build_operator` reports
    // it as unconfigured rather than reaching for ambient credentials.
    storage::Storage {
        s3_storage: base.s3_storage.clone().or(vended_s3),
        oss_storage: base.oss_storage.clone().or(vended_oss),
    }
}

fn prop(config: &StorageConfig, key: &str) -> Option<String> {
    config
        .get(key)
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn pair(config: &StorageConfig, id_key: &str, secret_key: &str) -> Option<(String, String)> {
    Some((prop(config, id_key)?, prop(config, secret_key)?))
}

fn path_style(config: &StorageConfig) -> bool {
    prop(config, S3_PATH_STYLE_ACCESS).is_some_and(|value| value.eq_ignore_ascii_case("true"))
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct LakeletIcebergStorage {
    storage: storage::Storage,
    /// Operators cached per `scheme://authority`. Iceberg locations may span
    /// several buckets or NameNodes, so operators are created lazily per
    /// authority instead of being bound to a single one.
    #[serde(skip)]
    operators: Arc<Mutex<HashMap<String, Operator>>>,
}

impl LakeletIcebergStorage {
    fn new(storage: storage::Storage) -> Self {
        Self {
            storage,
            operators: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Resolve a full location (`scheme://authority/path`) into the operator
    /// serving its authority and the storage-relative path.
    fn resolve(&self, location: &str) -> Result<(Operator, String)> {
        let (scheme, authority) =
            storage::parse_location_schema_authority(location).map_err(|error| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("Invalid storage location: {location}"),
                )
                .with_source(error)
            })?;

        let key = format!("{scheme}://{authority}");
        let operator = {
            let mut operators = self.operators.lock().unwrap();
            match operators.get(&key) {
                Some(op) => op.clone(),
                None => {
                    let op = self
                        .storage
                        .build_operator(&scheme, &authority)
                        .map_err(|error| {
                            Error::new(
                                ErrorKind::Unexpected,
                                format!("Failed to build operator for {key}"),
                            )
                            .with_source(error)
                        })?
                        .ok_or_else(|| {
                            Error::new(
                                ErrorKind::DataInvalid,
                                format!(
                                    "no storage configured for scheme '{scheme}' of {location}"
                                ),
                            )
                        })?;
                    operators.insert(key.clone(), op.clone());
                    op
                }
            }
        };

        let relative_path = get_relative_path(location, &key)?;
        Ok((operator, relative_path))
    }
}

#[async_trait]
#[typetag::serde]
impl Storage for LakeletIcebergStorage {
    async fn exists(&self, path: &str) -> Result<bool> {
        let (op, path) = self.resolve(path)?;
        op.exists(&path)
            .await
            .map_err(|error| from_opendal_error("check file existence", error))
    }

    async fn metadata(&self, path: &str) -> Result<FileMetadata> {
        let (op, path) = self.resolve(path)?;
        let meta = op
            .stat(&path)
            .await
            .map_err(|error| from_opendal_error("read file metadata", error))?;
        Ok(FileMetadata {
            size: meta.content_length(),
        })
    }

    async fn read(&self, path: &str) -> Result<Bytes> {
        let (op, path) = self.resolve(path)?;
        Ok(op
            .read(&path)
            .await
            .map_err(|error| from_opendal_error("read file", error))?
            .to_bytes())
    }

    async fn reader(&self, path: &str) -> Result<Box<dyn FileRead>> {
        let (op, path) = self.resolve(path)?;
        Ok(Box::new(LakeletFileReader(
            op.reader(&path)
                .await
                .map_err(|error| from_opendal_error("open file", error))?,
        )))
    }

    async fn write(&self, path: &str, bs: Bytes) -> Result<()> {
        let (op, path) = self.resolve(path)?;
        op.write(&path, bs)
            .await
            .map_err(|error| from_opendal_error("write file", error))?;
        Ok(())
    }

    async fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>> {
        let (op, path) = self.resolve(path)?;
        Ok(Box::new(LakeletFileWriter(
            op.writer(&path)
                .await
                .map_err(|error| from_opendal_error("open file for write", error))?,
        )))
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let (op, path) = self.resolve(path)?;
        op.delete(&path)
            .await
            .map_err(|error| from_opendal_error("delete file", error))
    }

    async fn delete_prefix(&self, path: &str) -> Result<()> {
        let (op, path) = self.resolve(path)?;
        let path = if path.ends_with('/') {
            path
        } else {
            format!("{path}/")
        };
        op.delete_with(&path)
            .recursive(true)
            .await
            .map_err(|error| from_opendal_error("delete prefix", error))
    }

    async fn delete_stream(&self, mut paths: BoxStream<'static, String>) -> Result<()> {
        // Paths may span several authorities; keep one deleter per operator.
        let mut deleters: HashMap<String, opendal::Deleter> = HashMap::new();
        while let Some(location) = paths.next().await {
            let (scheme, authority) =
                storage::parse_location_schema_authority(&location).map_err(|error| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!("Invalid storage location: {location}"),
                    )
                    .with_source(error)
                })?;
            let key = format!("{scheme}://{authority}");
            let (op, path) = self.resolve(&location)?;
            let deleter = match deleters.entry(key) {
                std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
                std::collections::hash_map::Entry::Vacant(entry) => entry.insert(
                    op.deleter()
                        .await
                        .map_err(|error| from_opendal_error("create deleter", error))?,
                ),
            };
            deleter
                .delete(path)
                .await
                .map_err(|error| from_opendal_error("delete file", error))?;
        }
        for (_, mut deleter) in deleters {
            deleter
                .close()
                .await
                .map_err(|error| from_opendal_error("close deleter", error))?;
        }
        Ok(())
    }

    fn new_input(&self, path: &str) -> Result<InputFile> {
        self.resolve(path)?;
        Ok(InputFile::new(Arc::new(self.clone()), path.to_string()))
    }

    fn new_output(&self, path: &str) -> Result<OutputFile> {
        self.resolve(path)?;
        Ok(OutputFile::new(Arc::new(self.clone()), path.to_string()))
    }
}

// Newtype wrappers: iceberg's FileRead/FileWrite cannot be implemented
// directly on opendal's Reader/Writer due to orphan rules.
struct LakeletFileReader(opendal::Reader);

#[async_trait]
impl FileRead for LakeletFileReader {
    async fn read(&self, range: Range<u64>) -> Result<Bytes> {
        Ok(self
            .0
            .read(range)
            .await
            .map_err(|error| from_opendal_error("read file range", error))?
            .to_bytes())
    }
}

struct LakeletFileWriter(opendal::Writer);

#[async_trait]
impl FileWrite for LakeletFileWriter {
    async fn write(&mut self, bs: Bytes) -> Result<()> {
        self.0
            .write(bs)
            .await
            .map_err(|error| from_opendal_error("write file", error))
    }

    async fn close(&mut self) -> Result<()> {
        let _ = self
            .0
            .close()
            .await
            .map_err(|error| from_opendal_error("close file", error))?;
        Ok(())
    }
}

/// Strip the `scheme://authority/` prefix and percent-decode the rest.
fn get_relative_path(location: &str, prefix: &str) -> Result<String> {
    let rest = location.strip_prefix(prefix).ok_or_else(|| {
        Error::new(
            ErrorKind::DataInvalid,
            format!("Location {location} does not start with {prefix}"),
        )
    })?;

    percent_decode_str(rest.trim_start_matches('/'))
        .decode_utf8()
        .map(|path| path.into_owned())
        .map_err(|error| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("Invalid storage path: {location}"),
            )
            .with_source(error)
        })
}

fn from_opendal_error(operation: &str, error: opendal::Error) -> Error {
    Error::new(ErrorKind::Unexpected, format!("Failed to {operation}")).with_source(error)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn resolve_relative(location: &str, prefix: &str) -> String {
        get_relative_path(location, prefix).unwrap()
    }

    #[test]
    fn test_relative_path_hdfs() {
        assert_eq!(
            resolve_relative(
                "hdfs://namenode:8020/warehouse/db/table/metadata.json",
                "hdfs://namenode:8020"
            ),
            "warehouse/db/table/metadata.json"
        );
    }

    #[test]
    fn test_relative_path_s3() {
        assert_eq!(
            resolve_relative("s3://bucket/warehouse/db/table/data.parquet", "s3://bucket"),
            "warehouse/db/table/data.parquet"
        );
    }

    #[test]
    fn test_relative_path_decodes_url_encoding() {
        assert_eq!(
            resolve_relative(
                "hdfs://namenode:8020/warehouse/table%20name/metadata.json",
                "hdfs://namenode:8020"
            ),
            "warehouse/table name/metadata.json"
        );
    }

    #[test]
    fn test_relative_path_rejects_prefix_mismatch() {
        let error =
            get_relative_path("s3://other/warehouse/data.parquet", "s3://bucket").unwrap_err();
        assert_eq!(error.kind(), ErrorKind::DataInvalid);
    }

    #[test]
    fn test_resolve_requires_storage_config() {
        let storage = LakeletIcebergStorage::new(storage::Storage::default());
        // A scheme with no block is reported as unconfigured rather than
        // signed with whatever credentials the environment happens to hold.
        for location in [
            "s3://bucket/warehouse/metadata.json",
            "s3a://bucket/warehouse/metadata.json",
            "oss://bucket/warehouse/metadata.json",
        ] {
            let error = storage.resolve(location).unwrap_err();
            assert_eq!(error.kind(), ErrorKind::DataInvalid, "{location}");
            assert!(
                error.to_string().contains("no storage configured"),
                "{location}"
            );
        }
    }

    #[test]
    fn test_resolve_hdfs_needs_no_config_and_caches_operator() {
        let storage = LakeletIcebergStorage::new(storage::Storage::default());
        let (op, path) = storage
            .resolve("hdfs://namenode:8020/warehouse/db/t/metadata.json")
            .unwrap();
        assert_eq!(op.info().scheme(), "hdfs-native");
        assert_eq!(path, "warehouse/db/t/metadata.json");
        assert_eq!(storage.operators.lock().unwrap().len(), 1);

        // A second authority gets its own operator.
        storage
            .resolve("hdfs://other:8020/warehouse/db/t/metadata.json")
            .unwrap();
        assert_eq!(storage.operators.lock().unwrap().len(), 2);
    }

    #[test]
    fn test_resolve_unsupported_scheme_errors() {
        let storage = LakeletIcebergStorage::new(storage::Storage::default());
        let error = storage
            .resolve("gcs://bucket/warehouse/metadata.json")
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Unexpected);
    }
}
