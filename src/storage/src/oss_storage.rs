use crate::storage::StorageTrait;
use datafusion::common::DataFusionError;
use datafusion::common::Result;
use datafusion::object_store::ObjectStore;
use opendal::Operator;
use opendal::services::Oss;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OSSStorage {
    #[serde(rename = "endpoint")]
    pub endpoint: Option<String>,
    #[serde(rename = "access-key")]
    pub access_key: Option<String>,
    #[serde(rename = "secret-key")]
    pub secret_key: Option<String>,
}

impl StorageTrait for OSSStorage {
    fn build_object_store(&self, bucket_name: &str) -> Result<Arc<dyn ObjectStore>> {
        let mut builder = Oss::default().bucket(bucket_name);
        if let Some(endpoint) = &self.endpoint {
            builder = builder.endpoint(endpoint);
        }
        if let Some(access_key) = &self.access_key {
            builder = builder.access_key_id(access_key);
        }
        if let Some(secret_key) = &self.secret_key {
            builder = builder.access_key_secret(secret_key);
        }
        let op = Operator::new(builder)
            .map_err(|err| DataFusionError::External(Box::new(err)))?
            .finish();
        Ok(Arc::new(object_store_opendal::OpendalStore::new(op)))
    }
}
