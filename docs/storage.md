# Storage

Storage configuration is attached to an HMS or Glue catalog and supplies the
properties needed to access table data.

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
| `region` | String | No | SDK default | AWS region used for requests. |
| `endpoint` | String | No | SDK default | Custom endpoint for S3-compatible services such as MinIO. |
| `access-key` | String | No | SDK default | Access key. |
| `secret-key` | String | No | SDK default | Secret key. |
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
| `endpoint` | String | No | SDK default | OSS endpoint. Must include the bucket name, for example `https://bucket_name.oss-cn-hangzhou.aliyuncs.com`. |
| `access-key` | String | No | SDK default | Static access key ID. |
| `secret-key` | String | No | SDK default | Static access key secret. |
| `path-style-access` | Boolean | No | `false` | Uses path-style requests when `true`; otherwise uses virtual-hosted-style requests. |

The OSS `endpoint` must be prefixed with the bucket name (`https://<bucket>.oss-<region>.aliyuncs.com`), not the bare regional endpoint.

```toml
[[catalog.hms]]
name = "hms"
metastore-uri = "127.0.0.1:9083"
oss-storage = { endpoint = "https://bucket_name.oss-cn-hangzhou.aliyuncs.com", access-key = "access-key", secret-key = "secret-key", path-style-access = false }
```