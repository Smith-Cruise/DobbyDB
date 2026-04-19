use crate::storage::StorageTrait;
use datafusion::common::Result;
use datafusion::error::DataFusionError;
use datafusion::object_store::ObjectStore;
use datafusion::object_store::aws::AmazonS3Builder;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3Storage {
    #[serde(rename = "region")]
    pub region: Option<String>,
    #[serde(rename = "endpoint")]
    pub endpoint: Option<String>,
    #[serde(rename = "access-key")]
    pub access_key: Option<String>,
    #[serde(rename = "secret-key")]
    pub secret_key: Option<String>,
}

impl StorageTrait for S3Storage {
    fn build_object_store(&self, bucket_name: &str) -> Result<Arc<dyn ObjectStore>> {
        let mut builder = AmazonS3Builder::new().with_bucket_name(bucket_name);
        if let Some(endpoint) = &self.endpoint {
            builder = builder.with_endpoint(endpoint);
            if endpoint.starts_with("http://") {
                builder = builder.with_allow_http(true);
            }
        }
        if let Some(region) = &self.region {
            builder = builder.with_region(region);
        }
        if let Some(access_key) = &self.access_key {
            builder = builder.with_access_key_id(access_key);
        }
        if let Some(secret_key) = &self.secret_key {
            builder = builder.with_secret_access_key(secret_key);
        }
        builder
            .build()
            .map(|store| Arc::new(store) as Arc<dyn ObjectStore>)
            .map_err(|err| DataFusionError::External(Box::new(err)))
    }
}
