use crate::oss_storage::OSSStorage;
use crate::s3_storage::S3Storage;
use datafusion::catalog::Session;
use datafusion::common::DataFusionError;
use datafusion::common::Result;
use datafusion::object_store::ObjectStore;
use deltalake::aws::constants::{
    AWS_ACCESS_KEY_ID, AWS_ENDPOINT_URL, AWS_REGION, AWS_SECRET_ACCESS_KEY,
};
use iceberg::io::{
    OSS_ACCESS_KEY_ID, OSS_ACCESS_KEY_SECRET, OSS_ENDPOINT, S3_ACCESS_KEY_ID, S3_ENDPOINT,
    S3_REGION, S3_SECRET_ACCESS_KEY,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use deltalake::aws::S3LogStoreFactory;
use deltalake::aws::storage::S3ObjectStoreFactory;
use deltalake::logstore::{logstore_factories, object_store_factories};
use url::Url;

pub trait StorageTrait {
    fn build_object_store(
        &self,
        bucket_name: &str,
    ) -> datafusion::common::Result<Arc<dyn ObjectStore>>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Storage {
    #[serde(rename = "s3-storage")]
    s3_storage: Option<S3Storage>,
    #[serde(rename = "oss-storage")]
    oss_storage: Option<OSSStorage>,
}

impl Storage {
    pub fn register_into_session(
        &self,
        table_location: impl Into<String>,
        session: &dyn Session,
    ) -> Result<()> {
        let table_location = table_location.into();
        let (path_schema, path_bucket) = parse_location_schema_bucket(&table_location)?;

        let object_store_path = Url::parse(&format!("{}://{}", path_schema, path_bucket))
            .map_err(|e| DataFusionError::External(e.into()))?;

        let registry = &session.runtime_env().object_store_registry;
        if let Some(s3_storage) = &self.s3_storage {
            registry.register_store(
                &object_store_path,
                s3_storage.build_object_store(&path_bucket)?,
            );
        }
        if let Some(oss_storage) = &self.oss_storage {
            registry.register_store(
                &object_store_path,
                oss_storage.build_object_store(&path_bucket)?,
            );
        }
        Ok(())
    }

    pub fn build_iceberg_file_io_properties(&self) -> HashMap<String, String> {
        let mut map: HashMap<String, String> = HashMap::new();
        if let Some(s3_storage) = &self.s3_storage {
            if let Some(region) = &s3_storage.region {
                map.insert(S3_REGION.into(), region.clone());
            }
            if let Some(endpoint) = &s3_storage.endpoint {
                map.insert(S3_ENDPOINT.into(), endpoint.clone());
            }
            if let Some(access_key) = &s3_storage.access_key {
                map.insert(S3_ACCESS_KEY_ID.into(), access_key.clone());
            }
            if let Some(secret_key) = &s3_storage.secret_key {
                map.insert(S3_SECRET_ACCESS_KEY.into(), secret_key.clone());
            }
        }

        if let Some(oss_storage) = &self.oss_storage {
            if let Some(endpoint) = &oss_storage.endpoint {
                map.insert(OSS_ENDPOINT.into(), endpoint.clone());
            }
            if let Some(access_key) = &oss_storage.access_key {
                map.insert(OSS_ACCESS_KEY_ID.into(), access_key.clone());
            }
            if let Some(secret_key) = &oss_storage.secret_key {
                map.insert(OSS_ACCESS_KEY_SECRET.into(), secret_key.clone());
            }
        }
        map
    }

    pub fn build_delta_storage_options(&self) -> HashMap<String, String> {
        let mut map: HashMap<String, String> = HashMap::new();
        if let Some(s3_storage) = &self.s3_storage {
            if let Some(region) = &s3_storage.region {
                map.insert(AWS_REGION.to_string(), region.clone());
            }
            if let Some(endpoint) = &s3_storage.endpoint {
                map.insert(AWS_ENDPOINT_URL.to_string(), endpoint.clone());
            }
            if let Some(access_key) = &s3_storage.access_key {
                map.insert(AWS_ACCESS_KEY_ID.to_string(), access_key.clone());
            }
            if let Some(secret_key) = &s3_storage.secret_key {
                map.insert(AWS_SECRET_ACCESS_KEY.to_string(), secret_key.clone());
            }
        }

        if let Some(oss_storage) = &self.oss_storage {
            if let Some(endpoint) = &oss_storage.endpoint {
                map.insert(AWS_ENDPOINT_URL.to_string(), endpoint.clone());
            }
            if let Some(access_key) = &oss_storage.access_key {
                map.insert(AWS_ACCESS_KEY_ID.to_string(), access_key.clone());
            }
            if let Some(secret_key) = &oss_storage.secret_key {
                map.insert(AWS_SECRET_ACCESS_KEY.to_string(), secret_key.clone());
            }
        }
        map
    }
}

pub fn parse_location_schema_bucket(path: &str) -> Result<(String, String)> {
    let parsed_url = Url::parse(path).map_err(|e| DataFusionError::External(e.into()))?;
    let url_schema = parsed_url.scheme();
    let bucket = if let Some(host) = parsed_url.host_str() {
        host
    } else {
        return Err(DataFusionError::Internal("failed to parse host".into()));
    };
    Ok((url_schema.to_string(), bucket.to_string()))
}

#[cfg(test)]
mod tests {
    use crate::storage::{Storage, parse_location_schema_bucket};

    #[test]
    fn test_parse_storage() {
        let text = r#"
            s3-storage = { endpoint = "http://127.0.0.1:9000", region = "us-east-1", access-key = "admin", secret-key = "password" }
        "#;

        let storages: Storage = toml::from_str(text).unwrap();
        assert!(storages.s3_storage.is_some());
        assert!(storages.oss_storage.is_none());
        let s3_storage = storages.s3_storage.unwrap();
        assert_eq!("http://127.0.0.1:9000", &s3_storage.endpoint.unwrap());
        assert_eq!("us-east-1", &s3_storage.region.unwrap());
        assert_eq!("admin", &s3_storage.access_key.unwrap());
        assert_eq!("password", &s3_storage.secret_key.unwrap());

        let text = r#"
            s3-storage = { endpoint = "http://127.0.0.1:9000", region = "us-east-1", access-key = "admin", secret-key = "password" }
            oss-storage = { endpoint = "http://127.0.0.1:9000", access-key = "admin", secret-key = "password" }
        "#;
        let storage: Storage = toml::from_str(text).unwrap();
        assert!(storage.s3_storage.is_some());
        assert!(storage.oss_storage.is_some());
        let oss_storage = storage.oss_storage.unwrap();
        assert_eq!("http://127.0.0.1:9000", &oss_storage.endpoint.unwrap());
        assert_eq!("admin", &oss_storage.access_key.unwrap());
        assert_eq!("password", &oss_storage.secret_key.unwrap());
    }

    #[test]
    fn test_parse_location_schema_bucket() {
        let (schema, bucket) =
            parse_location_schema_bucket("s3://bucket/tests/testdata/schema.json").unwrap();
        assert_eq!("s3", schema);
        assert_eq!("bucket", bucket);
    }
}
