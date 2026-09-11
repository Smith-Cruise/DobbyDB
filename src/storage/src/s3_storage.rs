use crate::storage::build_layered_operator;
use datafusion::common::Result;
use opendal::Operator;
use opendal::services::S3Config;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct S3Storage {
    #[serde(rename = "region")]
    pub region: Option<String>,
    #[serde(rename = "endpoint")]
    pub endpoint: Option<String>,
    #[serde(rename = "access-key")]
    pub access_key: Option<String>,
    #[serde(rename = "secret-key")]
    pub secret_key: Option<String>,
    #[serde(rename = "session-token")]
    pub session_token: Option<String>,
    #[serde(rename = "path-style-access", default)]
    pub path_style_access: bool,
}

impl S3Storage {
    pub fn build_operator(&self, bucket: &str) -> Result<Operator> {
        let mut cfg = S3Config::default();
        cfg.bucket = bucket.to_string();
        // When unset, OpenDAL falls back to AWS_REGION/AWS_DEFAULT_REGION
        // and errors with a clear message if those are missing too.
        cfg.region = self.region.clone();
        cfg.endpoint = self.endpoint.clone();
        cfg.access_key_id = self.access_key.clone();
        cfg.secret_access_key = self.secret_key.clone();
        cfg.session_token = self.session_token.clone();
        // OpenDAL defaults to path-style, the inverse of our config default.
        cfg.enable_virtual_host_style = !self.path_style_access;
        build_layered_operator(cfg)
    }
}
