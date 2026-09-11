use crate::storage::build_layered_operator;
use datafusion::common::Result;
use opendal::Operator;
use opendal::services::OssConfig;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct OSSStorage {
    #[serde(rename = "endpoint")]
    pub endpoint: Option<String>,
    #[serde(rename = "access-key")]
    pub access_key: Option<String>,
    #[serde(rename = "secret-key")]
    pub secret_key: Option<String>,
    #[serde(rename = "path-style-access", default)]
    pub path_style_access: bool,
}

impl OSSStorage {
    pub fn build_operator(&self, bucket: &str) -> Result<Operator> {
        let mut cfg = OssConfig::default();
        cfg.bucket = bucket.to_string();
        // The endpoint must not contain the bucket name; OpenDAL
        // prepends `{bucket}.` itself in virtual-hosted style.
        cfg.endpoint = self.endpoint.clone();
        cfg.access_key_id = self.access_key.clone();
        cfg.access_key_secret = self.secret_key.clone();
        cfg.addressing_style = Some(
            if self.path_style_access {
                "path"
            } else {
                "virtual"
            }
            .to_string(),
        );
        build_layered_operator(cfg)
    }
}
