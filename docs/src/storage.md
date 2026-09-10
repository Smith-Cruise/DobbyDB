---
icon: lucide/hard-drive
---

# Storage

Storage configuration is attached to catalogs and supplies the
credentials needed to access table data.

| Location scheme | Configuration |
| --- | --- |
| `s3://`, `s3a://` | `s3-storage` |
| `oss://` | `oss-storage` |
| `hdfs://` | No storage block; the NameNode authority comes from the location. |

Storage configuration is optional at the TOML level. Whether it can be omitted
in practice depends on the storage backend's authentication environment.

## AWS S3

Configure S3-compatible storage with the `s3-storage` inline table.

| Option | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `region` | String | No | `AWS_REGION` env | AWS region used for requests. Falls back to the `AWS_REGION` / `AWS_DEFAULT_REGION` environment variables; errors if none is available. |
| `endpoint` | String | No | AWS default | Custom endpoint for S3-compatible services such as MinIO. Must not include the bucket name. |
| `access-key` | String | No | Credential chain | Access key. When unset, the env/profile/IMDS credential chain is used. |
| `secret-key` | String | No | Credential chain | Secret key. |
| `session-token` | String | No | None | Session token for temporary (STS) credentials. |
| `path-style-access` | Boolean | No | `false` | Uses path-style requests when `true`; otherwise uses virtual-hosted-style requests. |

```toml
[[catalog.hms]]
name = "hms"
metastore-uri = "127.0.0.1:9083"
s3-storage = { region = "us-east-1", endpoint = "http://127.0.0.1:9000", access-key = "admin", secret-key = "password", path-style-access = true }
```

## Aliyun OSS

Configure Aliyun OSS with the `oss-storage` inline table.

| Option | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `endpoint` | String | Yes | None | OSS endpoint, for example `https://oss-cn-hangzhou.aliyuncs.com`. Must not include the bucket name. |
| `access-key` | String | No | None | Static access key ID. |
| `secret-key` | String | No | None | Static access key secret. |
| `path-style-access` | Boolean | No | `false` | Uses path-style requests when `true`; otherwise uses virtual-hosted-style requests. |

```toml
[[catalog.hms]]
name = "hms"
metastore-uri = "127.0.0.1:9083"
oss-storage = { endpoint = "https://oss-cn-hangzhou.aliyuncs.com", access-key = "access-key", secret-key = "secret-key", path-style-access = false }
```

## HDFS

Don't need to configure anything.

Kerberos is not supported yet.