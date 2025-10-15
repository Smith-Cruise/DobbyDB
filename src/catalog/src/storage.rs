use iceberg::io::{
    OSS_ACCESS_KEY_ID, OSS_ACCESS_KEY_SECRET, OSS_ENDPOINT, S3_ACCESS_KEY_ID, S3_ENDPOINT,
    S3_REGION, S3_SECRET_ACCESS_KEY,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageCredentials {
    #[serde(flatten)]
    s3_credential: Option<S3Credential>,
    #[serde(flatten)]
    oss_credential: Option<OSSCredential>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OSSCredential {
    #[serde(rename = "oss-endpoint")]
    pub oss_endpoint: Option<String>,
    #[serde(rename = "oss-access-key")]
    pub oss_access_key: Option<String>,
    #[serde(rename = "oss-secret-key")]
    pub oss_secret_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3Credential {
    #[serde(rename = "aws-s3-region")]
    pub aws_s3_region: Option<String>,
    #[serde(rename = "aws-s3-endpoint")]
    pub aws_s3_endpoint: Option<String>,
    #[serde(rename = "aws-s3-access-key")]
    pub aws_s3_access_key: Option<String>,
    #[serde(rename = "aws-s3-secret-key")]
    pub aws_s3_secret_key: Option<String>,
}

pub fn convert_storage_to_iceberg(
    storage_credentials: &StorageCredentials,
) -> HashMap<String, String> {
    let mut map: HashMap<String, String> = HashMap::new();
    if let Some(s3_credential) = &storage_credentials.s3_credential {
        if let Some(region) = &s3_credential.aws_s3_region {
            map.insert(S3_REGION.into(), region.clone());
        }
        if let Some(endpoint) = &s3_credential.aws_s3_endpoint {
            map.insert(S3_ENDPOINT.into(), endpoint.clone());
        }
        if let Some(access_key) = &s3_credential.aws_s3_access_key {
            map.insert(S3_ACCESS_KEY_ID.into(), access_key.clone());
        }
        if let Some(secret_key) = &s3_credential.aws_s3_secret_key {
            map.insert(S3_SECRET_ACCESS_KEY.into(), secret_key.clone());
        }
    }

    if let Some(oss_credential) = &storage_credentials.oss_credential {
        if let Some(endpoint) = &oss_credential.oss_endpoint {
            map.insert(OSS_ENDPOINT.into(), endpoint.clone());
        }
        if let Some(access_key) = &oss_credential.oss_access_key {
            map.insert(OSS_ACCESS_KEY_ID.into(), access_key.clone());
        }
        if let Some(secret_key) = &oss_credential.oss_secret_key {
            map.insert(OSS_ACCESS_KEY_SECRET.into(), secret_key.clone());
        }
    }

    map
}
