---
icon: lucide/database
---

# Catalog

Lakelet supports these catalogs:

- [HMS](hms.md)
- [Glue](glue.md)
- [Paimon FileSystem](paimon-filesystem.md)
- [Iceberg REST](iceberg-rest.md)

Storage settings are embedded directly in each catalog entry. See
[Storage](../storage.md) for their complete field reference.

## Catalog SQL

Lakelet supports these catalog-related SQL statements:

```sql
-- List all catalogs.
show catalogs;

-- Change the current catalog.
use catalog hms_prod;

-- Change both the current catalog and current database.
use hms_prod.default;
```

## Multiple catalogs

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

[[catalog.paimon-fs]]
name = "paimon_lake"
warehouse = "s3://bucket/warehouse"
```
