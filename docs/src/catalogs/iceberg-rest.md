---
icon: lucide/database
---

# Iceberg REST

Use `[[catalog.iceberg-rest]]` to access existing Iceberg tables through a REST
catalog. Lakelet reuses `iceberg-rust` for REST requests and reads.

| Option | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `name` | String | Yes | None | Unique Lakelet catalog name. |
| `uri` | String | Yes | None | HTTP(S) REST catalog base URL. Do not include credentials or query parameters. |
| `warehouse` | String | No | None | Warehouse identifier, passed unchanged to the service. |
| `token` | String | No | None | Bearer token for authenticating requests to the REST service. |

An open REST catalog needs no token:

```toml
[[catalog.iceberg-rest]]
name = "iceberg_prod"
uri = "http://localhost:8181"
warehouse = "warehouse_name"
```

For a service requiring bearer authentication, add `token` directly to the same
catalog block:

```toml
[[catalog.iceberg-rest]]
name = "iceberg_prod"
uri = "https://catalog.example.com"
warehouse = "warehouse_name"
token = "<REST catalog access token>"
```

## Storage credentials

Lakelet resolves credentials per location scheme, in this order:

1. This catalog's own `s3-storage` / `oss-storage` block. It suppresses the vended
   credentials for that scheme.
2. Credentials vended by the catalog, when the block above is absent.
3. The backend's own credential chain (environment variables, shared profile,
   instance metadata), when neither of the above supplies the scheme.

```toml
[[catalog.iceberg-rest]]
name = "iceberg_prod"
uri = "https://catalog.example.com"
warehouse = "warehouse_name"
s3-storage = { region = "us-east-1", access-key = "<access key>", secret-key = "<secret key>" }
```