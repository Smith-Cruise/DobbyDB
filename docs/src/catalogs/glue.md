---
icon: lucide/cloud
---

# Glue

Use `[[catalog.glue]]` to connect to the AWS Glue Data Catalog.

| Option | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `name` | String | Yes | None | Unique catalog name used by Lakelet. |
| `aws-glue-region` | String | No | AWS SDK default | AWS region containing the Glue catalog. |
| `aws-glue-access-key` | String | No | AWS SDK default credential chain | Static access key used for Glue requests. |
| `aws-glue-secret-key` | String | No | AWS SDK default credential chain | Static secret key used for Glue requests. |
| `aws-glue-endpoint` | String | No | AWS SDK default | Custom endpoint URL used for Glue API requests. |

Static Glue credentials are used only when both `aws-glue-access-key` and
`aws-glue-secret-key` are present. If either value is missing, the AWS SDK
default credential chain and region resolution are used.

```toml
[[catalog.glue]]
name = "glue_prod"
aws-glue-region = "us-west-2"
aws-glue-access-key = "access-key"
aws-glue-secret-key = "secret-key"
aws-glue-endpoint = "http://127.0.0.1:4566"
s3-storage = { region = "us-west-2", access-key = "access-key", secret-key = "secret-key" }
```
