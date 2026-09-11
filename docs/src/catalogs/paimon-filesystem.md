---
icon: lucide/folder-tree
---

# Paimon FileSystem

Use `[[catalog.paimon-fs]]` to connect to a
[Paimon filesystem catalog](https://paimon.apache.org/docs/master/concepts/catalog/):
a warehouse directory that stores all metadata directly on the filesystem,
without a metastore service. This catalog serves Paimon tables only.

| Option | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `name` | String | Yes | None | Unique catalog name used by Lakelet. |
| `warehouse` | String | Yes | None | Warehouse root path, e.g. `s3://bucket/warehouse` or `file:///data/warehouse`. |

```toml
[[catalog.paimon-fs]]
name = "paimon_prod"
warehouse = "s3://bucket/warehouse"
s3-storage = { region = "us-east-1", access-key = "access-key", secret-key = "secret-key" }
```

A local warehouse uses a `file://` URI and needs no storage block at all
(a bare absolute path is also accepted):

```toml
[[catalog.paimon-fs]]
name = "paimon_local"
warehouse = "file:///data/paimon/warehouse"
```

The warehouse follows the Paimon filesystem catalog layout: each database is a
`<database>.db` directory under the warehouse, and each table a directory
beneath it (`warehouse/<database>.db/<table>`). Databases and tables are
discovered by listing those directories, so `show schemas` reports every
`*.db` directory — including non-Paimon ones.
