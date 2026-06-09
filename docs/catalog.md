# Catalog

DobbyDB support to connect HMS and Glue catalog.

Storage settings are embedded directly in each catalog entry. See
[Storage](storage.md) for their complete field reference.

## HMS

Use `[[catalog.hms]]` to connect to a Hive Metastore through Thrift.

| Option | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `name` | String | Yes | None | Unique catalog name used by DobbyDB. |
| `metastore-uri` | String | Yes | None | Hive Metastore address in `host:port` form. |
| `s3-storage` | Inline table | No | Not configured | Credentials and endpoint for tables stored at `s3://` or `s3a://` locations. |
| `oss-storage` | Inline table | No | Not configured | Credentials and endpoint for tables stored at `oss://` locations. |

```toml
[[catalog.hms]]
name = "hms_prod"
metastore-uri = "hms.example.com:9083"
s3-storage = { region = "us-east-1", access-key = "access-key", secret-key = "secret-key" }
```

The metastore URI is resolved as a socket address when the catalog is accessed.
It must not include a URI scheme such as `thrift://`.

Hive tables stored at `hdfs://` locations do not need a storage configuration
block. DobbyDB reads the NameNode host and port from the table location. Kerberos
is not support yet.

## Glue

Use `[[catalog.glue]]` to connect to the AWS Glue Data Catalog.

| Option | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `name` | String | Yes | None | Unique catalog name used by DobbyDB. |
| `aws-glue-region` | String | No | AWS SDK default | AWS region containing the Glue catalog. |
| `aws-glue-access-key` | String | No | AWS SDK default credential chain | Static access key used for Glue requests. |
| `aws-glue-secret-key` | String | No | AWS SDK default credential chain | Static secret key used for Glue requests. |
| `s3-storage` | Inline table | No | Not configured | Credentials and endpoint used to read table data at `s3://` or `s3a://` locations. |
| `oss-storage` | Inline table | No | Not configured | Credentials and endpoint used to read table data at `oss://` locations. |

Static Glue credentials are used only when both `aws-glue-access-key` and
`aws-glue-secret-key` are present. If either value is missing, the AWS SDK
default credential chain and region resolution are used.

```toml
[[catalog.glue]]
name = "glue_prod"
aws-glue-region = "us-west-2"
aws-glue-access-key = "access-key"
aws-glue-secret-key = "secret-key"
s3-storage = { region = "us-west-2", access-key = "access-key", secret-key = "secret-key" }
```

Catalog blocks are repeatable, you can create multiple catalogs like:

```toml
[[catalog.hms]]
name = "hms_dev"
metastore-uri = "127.0.0.1:9083"

[[catalog.hms]]
name = "hms_prod"
metastore-uri = "hms.example.com:9083"

[[catalog.glue]]
name = "glue_analytics"
aws-glue-region = "us-east-1"
```
