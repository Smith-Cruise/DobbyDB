use crate::hdfs_storage;
use crate::oss_storage::OSSStorage;
use crate::s3_storage::S3Storage;
use datafusion::catalog::Session;
use datafusion::common::DataFusionError;
use datafusion::common::Result;
use datafusion::object_store::ObjectStore;
use object_store_opendal::OpendalStore;
use opendal::Operator;
use opendal::layers::{RetryLayer, TimeoutLayer};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use url::Url;

pub const S3_SCHEMA: &str = "s3";
pub const S3A_SCHEMA: &str = "s3a";
pub const OSS_SCHEMA: &str = "oss";
pub const HDFS_SCHEMA: &str = "hdfs";

/// Per-catalog storage configuration. A catalog without any storage block
/// deserializes to the default value (all backends unset); HDFS needs no
/// configuration at all.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Storage {
    #[serde(rename = "s3-storage")]
    pub s3_storage: Option<S3Storage>,
    #[serde(rename = "oss-storage")]
    pub oss_storage: Option<OSSStorage>,
}

impl Storage {
    /// Build an OpenDAL operator rooted at the storage's top level (bucket
    /// root for s3/oss, `/` for hdfs). This is the single place where a
    /// location scheme + authority is mapped onto a storage backend and
    /// credentials.
    ///
    /// Returns `Ok(None)` when no storage backend is configured for the
    /// scheme; callers decide whether that is an error. Unsupported schemes
    /// fail right away.
    pub fn build_operator(&self, scheme: &str, authority: &str) -> Result<Option<Operator>> {
        let operator = match scheme {
            S3_SCHEMA | S3A_SCHEMA => self
                .s3_storage
                .as_ref()
                .map(|s3_storage| s3_storage.build_operator(authority))
                .transpose()?,
            OSS_SCHEMA => self
                .oss_storage
                .as_ref()
                .map(|oss_storage| oss_storage.build_operator(authority))
                .transpose()?,
            HDFS_SCHEMA => Some(hdfs_storage::build_operator(authority)?),
            _ => {
                return Err(DataFusionError::NotImplemented(format!(
                    "unsupported storage scheme: {scheme}"
                )));
            }
        };
        Ok(operator)
    }

    /// Build a root-level `ObjectStore` for DataFusion's registry (and
    /// Delta's storage backend) on top of the unified OpenDAL operator.
    pub fn build_root_object_store(
        &self,
        scheme: &str,
        authority: &str,
    ) -> Result<Option<Arc<dyn ObjectStore>>> {
        Ok(self
            .build_operator(scheme, authority)?
            .map(|op| Arc::new(OpendalStore::new(op)) as Arc<dyn ObjectStore>))
    }

    pub fn build_paimon_file_io_properties(&self) -> HashMap<String, String> {
        let mut map = HashMap::new();
        if let Some(s3_storage) = &self.s3_storage {
            if let Some(region) = &s3_storage.region {
                map.insert("s3.region".to_string(), region.clone());
            } else {
                map.insert("s3.region".to_string(), "us-east-1".to_string());
            }
            if let Some(endpoint) = &s3_storage.endpoint {
                map.insert("s3.endpoint".to_string(), endpoint.clone());
            }
            if let Some(access_key) = &s3_storage.access_key {
                map.insert("s3.access-key".to_string(), access_key.clone());
            }
            if let Some(secret_key) = &s3_storage.secret_key {
                map.insert("s3.secret-key".to_string(), secret_key.clone());
            }
            map.insert(
                "s3.path-style-access".to_string(),
                s3_storage.path_style_access.to_string(),
            );
        }

        if let Some(oss_storage) = &self.oss_storage {
            if let Some(endpoint) = &oss_storage.endpoint {
                map.insert("fs.oss.endpoint".to_string(), endpoint.clone());
            }
            if let Some(access_key) = &oss_storage.access_key {
                map.insert("fs.oss.accessKeyId".to_string(), access_key.clone());
            }
            if let Some(secret_key) = &oss_storage.secret_key {
                map.insert("fs.oss.accessKeySecret".to_string(), secret_key.clone());
            }
        }
        map
    }
}

pub(crate) fn build_layered_operator<C: opendal::Configurator>(cfg: C) -> Result<Operator> {
    // TimeoutLayer must sit inside RetryLayer so each retry attempt gets an
    // independent timeout; a hung request would otherwise never be retried.
    // RetryLayer is what turns transient HTTP failures (e.g. a stale pooled
    // connection closed by the server) into silent retries instead of query
    // errors.
    Ok(Operator::from_config(cfg)
        .map_err(|err| DataFusionError::External(Box::new(err)))?
        .layer(TimeoutLayer::new())
        .layer(RetryLayer::new()))
}

pub fn try_register_storage_info_session(
    storage: &Storage,
    table_location: impl Into<String>,
    session: &dyn Session,
) -> Result<()> {
    let table_location = table_location.into();
    let (path_schema, path_bucket) = parse_location_schema_authority(&table_location)?;

    let object_store_path = Url::parse(&format!("{}://{}", path_schema, path_bucket))
        .map_err(|e| DataFusionError::External(e.into()))?;

    let registry = &session.runtime_env().object_store_registry;
    if registry.get_store(&object_store_path).is_ok() {
        return Ok(());
    }

    if let Some(store) = storage.build_root_object_store(&path_schema, &path_bucket)? {
        registry.register_store(&object_store_path, store);
    }
    Ok(())
}

pub fn parse_location_schema_authority(path: &str) -> Result<(String, String)> {
    let parsed_url = Url::parse(path).map_err(|e| DataFusionError::External(e.into()))?;
    let url_schema = parsed_url.scheme();
    // Use the full authority (host:port) so HDFS NameNode ports are preserved.
    // For s3/oss locations without a port, this is identical to the host.
    let authority = parsed_url.authority();
    if authority.is_empty() {
        return Err(DataFusionError::Internal(
            "failed to parse authority".into(),
        ));
    }
    Ok((url_schema.to_string(), authority.to_string()))
}

#[cfg(test)]
mod tests {
    use crate::storage::*;
    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion::prelude::SessionContext;

    const S3_TOML: &str = r#"
        s3-storage = { endpoint = "http://127.0.0.1:9000", region = "cn-north-1", access-key = "ak", secret-key = "sk", session-token = "token", path-style-access = true }
    "#;

    const OSS_TOML: &str = r#"
        oss-storage = { endpoint = "https://oss-cn-hangzhou.aliyuncs.com", access-key = "ak", secret-key = "sk" }
    "#;

    fn parse_toml(text: &str) -> Storage {
        toml::from_str(text).unwrap()
    }

    #[test]
    fn test_build_operator_s3() {
        let storage = parse_toml(S3_TOML);
        for scheme in [S3_SCHEMA, S3A_SCHEMA] {
            let op = storage
                .build_operator(scheme, "bucket")
                .unwrap()
                .expect("s3 operator should be built");
            assert_eq!(op.info().name(), "bucket");
            assert_eq!(op.info().scheme(), "s3");
        }
    }

    #[test]
    fn test_build_operator_oss() {
        let storage = parse_toml(OSS_TOML);
        let op = storage
            .build_operator(OSS_SCHEMA, "bucket")
            .unwrap()
            .expect("oss operator should be built");
        assert_eq!(op.info().name(), "bucket");
        assert_eq!(op.info().scheme(), "oss");
    }

    #[test]
    fn test_build_operator_hdfs_needs_no_config() {
        let op = Storage::default()
            .build_operator(HDFS_SCHEMA, "namenode:8020")
            .unwrap()
            .expect("hdfs operator should be built without storage config");
        assert_eq!(op.info().scheme(), "hdfs-native");
    }

    #[test]
    fn test_build_operator_missing_storage_config() {
        assert!(
            Storage::default()
                .build_operator(S3_SCHEMA, "bucket")
                .unwrap()
                .is_none()
        );
        let storage = parse_toml(S3_TOML);
        assert!(
            storage
                .build_operator(OSS_SCHEMA, "bucket")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn test_build_operator_unsupported_scheme_errors() {
        let storage = parse_toml(S3_TOML);
        let err = storage.build_operator("gcs", "bucket").unwrap_err();
        assert!(err.to_string().contains("unsupported storage scheme: gcs"));
    }

    #[test]
    fn test_build_root_object_store() {
        let storage = parse_toml(S3_TOML);
        assert!(
            storage
                .build_root_object_store(S3_SCHEMA, "bucket")
                .unwrap()
                .is_some()
        );
        assert!(storage.build_root_object_store("gcs", "bucket").is_err());
    }

    #[test]
    fn test_parse_storage() {
        // Every field of both backends.
        let storage = parse_toml(
            r#"
            s3-storage = { region = "us-east-1", endpoint = "http://127.0.0.1:9000", access-key = "s3-ak", secret-key = "s3-sk", session-token = "s3-token", path-style-access = true }
            oss-storage = { endpoint = "https://oss-cn-hangzhou.aliyuncs.com", access-key = "oss-ak", secret-key = "oss-sk", path-style-access = true }
        "#,
        );
        let s3_storage = storage.s3_storage.unwrap();
        assert_eq!("us-east-1", &s3_storage.region.unwrap());
        assert_eq!("http://127.0.0.1:9000", &s3_storage.endpoint.unwrap());
        assert_eq!("s3-ak", &s3_storage.access_key.unwrap());
        assert_eq!("s3-sk", &s3_storage.secret_key.unwrap());
        assert_eq!("s3-token", &s3_storage.session_token.unwrap());
        assert!(s3_storage.path_style_access);
        let oss_storage = storage.oss_storage.unwrap();
        assert_eq!(
            "https://oss-cn-hangzhou.aliyuncs.com",
            &oss_storage.endpoint.unwrap()
        );
        assert_eq!("oss-ak", &oss_storage.access_key.unwrap());
        assert_eq!("oss-sk", &oss_storage.secret_key.unwrap());
        assert!(oss_storage.path_style_access);

        // Every field is optional, and path-style-access defaults to false.
        let storage = parse_toml("s3-storage = {}\noss-storage = {}\n");
        let s3_storage = storage.s3_storage.unwrap();
        assert!(s3_storage.region.is_none());
        assert!(s3_storage.endpoint.is_none());
        assert!(s3_storage.access_key.is_none());
        assert!(s3_storage.secret_key.is_none());
        assert!(s3_storage.session_token.is_none());
        assert!(!s3_storage.path_style_access);
        let oss_storage = storage.oss_storage.unwrap();
        assert!(oss_storage.endpoint.is_none());
        assert!(oss_storage.access_key.is_none());
        assert!(oss_storage.secret_key.is_none());
        assert!(!oss_storage.path_style_access);

        // A catalog carrying no storage block at all.
        let storage = parse_toml("");
        assert!(storage.s3_storage.is_none());
        assert!(storage.oss_storage.is_none());
    }

    #[test]
    fn test_build_paimon_file_io_properties() {
        let text = r#"
            s3-storage = { endpoint = "http://127.0.0.1:9000", region = "us-east-1", access-key = "admin", secret-key = "password", path-style-access = true }
            oss-storage = { endpoint = "https://oss.example.com", access-key = "oss-ak", secret-key = "oss-sk" }
        "#;

        let storage: Storage = toml::from_str(text).unwrap();
        let properties = storage.build_paimon_file_io_properties();

        assert_eq!(properties["s3.endpoint"], "http://127.0.0.1:9000");
        assert_eq!(properties["s3.region"], "us-east-1");
        assert_eq!(properties["s3.access-key"], "admin");
        assert_eq!(properties["s3.secret-key"], "password");
        assert_eq!(properties["s3.path-style-access"], "true");
        assert_eq!(properties["fs.oss.endpoint"], "https://oss.example.com");
        assert_eq!(properties["fs.oss.accessKeyId"], "oss-ak");
        assert_eq!(properties["fs.oss.accessKeySecret"], "oss-sk");
    }

    #[test]
    fn test_parse_location_schema_authority() {
        let (schema, bucket) =
            parse_location_schema_authority("s3://bucket/tests/testdata/schema.json").unwrap();
        assert_eq!("s3", schema);
        assert_eq!("bucket", bucket);

        let (schema, bucket) =
            parse_location_schema_authority("hdfs://namenode:8020/user/hive/warehouse/db/t")
                .unwrap();
        assert_eq!("hdfs", schema);
        assert_eq!("namenode:8020", bucket);
    }

    #[test]
    fn test_register_hdfs_without_storage_config() {
        let ctx = SessionContext::new();

        try_register_storage_info_session(
            &Storage::default(),
            "hdfs://namenode:8020/user/hive/warehouse/db/t",
            &ctx.state(),
        )
        .unwrap();

        assert!(
            ctx.runtime_env()
                .object_store(ObjectStoreUrl::parse("hdfs://namenode:8020").unwrap())
                .is_ok()
        );
    }
}
