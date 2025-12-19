use datafusion::catalog::Session;
use datafusion::error::DataFusionError;
use datafusion::object_store::aws::AmazonS3Builder;
use datafusion::object_store::ObjectStore;
use deltalake_aws::constants::{
    AWS_ACCESS_KEY_ID, AWS_ENDPOINT_URL, AWS_REGION, AWS_SECRET_ACCESS_KEY,
};
use iceberg::io::{
    OSS_ACCESS_KEY_ID, OSS_ACCESS_KEY_SECRET, OSS_ENDPOINT, S3_ACCESS_KEY_ID, S3_ENDPOINT,
    S3_REGION, S3_SECRET_ACCESS_KEY,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use url::Url;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageCredential {
    #[serde(rename = "s3-credential")]
    s3_credential: Option<S3Credential>,
    #[serde(rename = "oss-credential")]
    oss_credential: Option<OSSCredential>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OSSCredential {
    #[serde(rename = "endpoint")]
    pub endpoint: Option<String>,
    #[serde(rename = "access-key")]
    pub access_key: Option<String>,
    #[serde(rename = "secret-key")]
    pub secret_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3Credential {
    #[serde(rename = "region")]
    pub region: Option<String>,
    #[serde(rename = "endpoint")]
    pub endpoint: Option<String>,
    #[serde(rename = "access-key")]
    pub access_key: Option<String>,
    #[serde(rename = "secret-key")]
    pub secret_key: Option<String>,
}

impl StorageCredential {
    pub fn register_into_session(
        &self,
        table_location: impl Into<String>,
        session: &dyn Session,
    ) -> Result<(), DataFusionError> {
        let table_location = table_location.into();
        let (schema, host) = parse_location_schema_host(&table_location)?;

        let object_store_path = Url::parse(&format!("{}://{}", schema, host))
            .map_err(|e| DataFusionError::External(e.into()))?;

        let registry = &session.runtime_env().object_store_registry;
        if let Some(s3_credential) = &self.s3_credential {
            registry.register_store(
                &object_store_path,
                s3_credential.build_object_store(table_location)?,
            );
        }
        if let Some(oss_credential) = &self.oss_credential {
            let oss_url = Url::parse("oss://").map_err(|e| DataFusionError::External(e.into()))?;
            registry.register_store(&oss_url, oss_credential.build_object_store()?);
        }
        Ok(())
    }

    pub fn build_iceberg_file_io_properties(&self) -> HashMap<String, String> {
        let mut map: HashMap<String, String> = HashMap::new();
        if let Some(s3_credential) = &self.s3_credential {
            if let Some(region) = &s3_credential.region {
                map.insert(S3_REGION.into(), region.clone());
            }
            if let Some(endpoint) = &s3_credential.endpoint {
                map.insert(S3_ENDPOINT.into(), endpoint.clone());
            }
            if let Some(access_key) = &s3_credential.access_key {
                map.insert(S3_ACCESS_KEY_ID.into(), access_key.clone());
            }
            if let Some(secret_key) = &s3_credential.secret_key {
                map.insert(S3_SECRET_ACCESS_KEY.into(), secret_key.clone());
            }
        }

        if let Some(oss_credential) = &self.oss_credential {
            if let Some(endpoint) = &oss_credential.endpoint {
                map.insert(OSS_ENDPOINT.into(), endpoint.clone());
            }
            if let Some(access_key) = &oss_credential.access_key {
                map.insert(OSS_ACCESS_KEY_ID.into(), access_key.clone());
            }
            if let Some(secret_key) = &oss_credential.secret_key {
                map.insert(OSS_ACCESS_KEY_SECRET.into(), secret_key.clone());
            }
        }
        map
    }

    pub fn build_delta_storage_options(&self) -> HashMap<String, String> {
        let mut map: HashMap<String, String> = HashMap::new();
        if let Some(s3_credential) = &self.s3_credential {
            if let Some(region) = &s3_credential.region {
                map.insert(AWS_REGION.to_string(), region.clone());
            }
            if let Some(endpoint) = &s3_credential.endpoint {
                map.insert(AWS_ENDPOINT_URL.to_string(), endpoint.clone());
            }
            if let Some(access_key) = &s3_credential.access_key {
                map.insert(AWS_ACCESS_KEY_ID.to_string(), access_key.clone());
            }
            if let Some(secret_key) = &s3_credential.secret_key {
                map.insert(AWS_SECRET_ACCESS_KEY.to_string(), secret_key.clone());
            }
        }

        if let Some(oss_credential) = &self.oss_credential {
            if let Some(endpoint) = &oss_credential.endpoint {
                map.insert(AWS_ENDPOINT_URL.to_string(), endpoint.clone());
            }
            if let Some(access_key) = &oss_credential.access_key {
                map.insert(AWS_ACCESS_KEY_ID.to_string(), access_key.clone());
            }
            if let Some(secret_key) = &oss_credential.secret_key {
                map.insert(AWS_SECRET_ACCESS_KEY.to_string(), secret_key.clone());
            }
        }
        map
    }
}

impl S3Credential {
    fn build_object_store(
        &self,
        table_location: impl Into<String>,
    ) -> Result<Arc<dyn ObjectStore>, DataFusionError> {
        let mut s3_builder = AmazonS3Builder::new();
        s3_builder = s3_builder.with_url(table_location);
        s3_builder = s3_builder.with_allow_http(true);
        if let Some(region) = &self.region {
            s3_builder = s3_builder.with_region(region);
        }
        if let Some(endpoint) = &self.endpoint {
            s3_builder = s3_builder.with_endpoint(endpoint);
        }
        if let Some(access_key) = &self.access_key {
            s3_builder = s3_builder.with_access_key_id(access_key);
        }
        if let Some(secret_key) = &self.secret_key {
            s3_builder = s3_builder.with_secret_access_key(secret_key);
        }
        s3_builder = s3_builder.with_bucket_name("warehouse");
        Ok(Arc::new(s3_builder.build()?))
    }
}

impl OSSCredential {
    fn build_object_store(&self) -> Result<Arc<dyn ObjectStore>, DataFusionError> {
        let mut s3_builder = AmazonS3Builder::new();
        if let Some(endpoint) = &self.endpoint {
            s3_builder = s3_builder.with_endpoint(endpoint);
        }
        if let Some(access_key) = &self.access_key {
            s3_builder = s3_builder.with_access_key_id(access_key);
        }
        if let Some(secret_key) = &self.secret_key {
            s3_builder = s3_builder.with_secret_access_key(secret_key);
        }
        Ok(Arc::new(s3_builder.build()?))
    }
}

pub fn parse_location_schema_host(path: &str) -> Result<(String, String), DataFusionError> {
    let parsed_url = Url::parse(&path).map_err(|e| DataFusionError::External(e.into()))?;
    let url_schema = parsed_url.scheme();
    let host = if let Some(host) = parsed_url.host_str() {
        host
    } else {
        return Err(DataFusionError::Internal("failed to parse host".into()));
    };
    Ok((url_schema.to_string(), host.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_storage_credential() {
        let text = r#"
            s3-credential = { endpoint = "http://127.0.0.1:9000", region = "us-east-1", access-key = "admin", secret-key = "password" }
        "#;

        let storage_credential: StorageCredential = toml::from_str(text).unwrap();
        assert!(storage_credential.s3_credential.is_some());
        assert!(storage_credential.oss_credential.is_none());
        let s3_credential = storage_credential.s3_credential.unwrap();
        assert_eq!("http://127.0.0.1:9000", &s3_credential.endpoint.unwrap());
        assert_eq!("us-east-1", &s3_credential.region.unwrap());
        assert_eq!("admin", &s3_credential.access_key.unwrap());
        assert_eq!("password", &s3_credential.secret_key.unwrap());

        let text = r#"
            s3-credential = { endpoint = "http://127.0.0.1:9000", region = "us-east-1", access-key = "admin", secret-key = "password" }
            oss-credential = { endpoint = "http://127.0.0.1:9000", access-key = "admin", secret-key = "password" }
        "#;
        let storage_credential: StorageCredential = toml::from_str(text).unwrap();
        assert!(storage_credential.s3_credential.is_some());
        assert!(storage_credential.oss_credential.is_some());
        let oss_credential = storage_credential.oss_credential.unwrap();
        assert_eq!("http://127.0.0.1:9000", &oss_credential.endpoint.unwrap());
        assert_eq!("admin", &oss_credential.access_key.unwrap());
        assert_eq!("password", &oss_credential.secret_key.unwrap());
    }

    #[test]
    fn test_parse_location_schema_host() {
        let (schema, host) =
            parse_location_schema_host("s3://bucket/tests/testdata/schema.json").unwrap();
        assert_eq!("s3", schema);
        assert_eq!("bucket", host);
    }
}
