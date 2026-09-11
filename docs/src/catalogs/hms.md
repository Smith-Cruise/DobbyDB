---
icon: lucide/warehouse
---

# HMS

Use `[[catalog.hms]]` to connect to a Hive Metastore through Thrift.

| Option | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `name` | String | Yes | None | Unique catalog name used by Lakelet. |
| `metastore-uri` | String | Yes | None | Hive Metastore address in `host:port` form. |

```toml
[[catalog.hms]]
name = "hms_prod"
metastore-uri = "hms.example.com:9083"
s3-storage = { region = "us-east-1", access-key = "access-key", secret-key = "secret-key" }
```

The metastore URI is resolved as a socket address when the catalog is accessed.
It must not include a URI scheme such as `thrift://`.