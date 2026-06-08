# Storage

Storage configuration is attached to an HMS or Glue catalog and supplies the
properties needed to access table data. DobbyDB selects the configuration from
the scheme of the table or metadata location.

```toml
[[catalog.hms]]
name = "hms"
metastore-uri = "127.0.0.1:9083"
s3-storage = { region = "us-east-1" }
```

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
| `region` | String | No | Backend or SDK default | AWS region used for requests. |
| `endpoint` | String | No | Backend default | Custom endpoint for S3-compatible services such as MinIO. |
| `access-key` | String | No | Backend credential resolution | Static access key. |
| `secret-key` | String | No | Backend credential resolution | Static secret key. |
| `path-style-access` | Boolean | No | `false` | Uses path-style requests when `true`; otherwise uses virtual-hosted-style requests. |

```toml
[[catalog.hms]]
name = "hms"
metastore-uri = "127.0.0.1:9083"
s3-storage = { region = "us-east-1", endpoint = "http://127.0.0.1:9000", access-key = "admin", secret-key = "password", path-style-access = true }
```

HTTP is enabled automatically when `endpoint` starts with `http://`.

## Aliyun OSS

Configure Aliyun OSS with the `oss-storage` inline table.

| Option | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `endpoint` | String | No | Backend default | OSS endpoint, for example `https://oss-cn-hangzhou.aliyuncs.com`. |
| `access-key` | String | No | Backend credential resolution | Static access key ID. |
| `secret-key` | String | No | Backend credential resolution | Static access key secret. |
| `path-style-access` | Boolean | No | `false` | Uses path-style requests when `true`; otherwise uses virtual-hosted-style requests. |

```toml
[[catalog.hms]]
name = "hms"
metastore-uri = "127.0.0.1:9083"
oss-storage = { endpoint = "https://oss-cn-hangzhou.aliyuncs.com", access-key = "access-key", secret-key = "secret-key", path-style-access = false }
```

HTTP is enabled automatically when `endpoint` starts with `http://`.
