# Table Format

DobbyDB determines a table format from metastore table properties. Table
formats are discovered through HMS or Glue; there is no separate table-format
configuration block.

| Capability | Hive | Iceberg | Delta Lake |
| --- | --- | --- | --- |
| Status | Supported with documented limits | Supported with documented limits | Incomplete support |
| Detection | Fallback when no Iceberg or Delta marker is present | `metadata_location` property exists | `spark.sql.sources.provider` equals `DELTA` |
| Read table data | Yes | Yes | Not considered fully supported |
| File formats | TextFile, Parquet | Determined by Iceberg implementation; current project focus is Parquet | Not documented as stable |
| Partitioned tables | Yes, including partition pruning | Yes | Not documented as stable |
| Metadata tables | `$file_path` | `$snapshots`, `$manifests` | No documented support |
| Storage schemes | `s3`, `s3a`, `oss`, `hdfs` | `file`, `s3`, `s3a`, `oss` | Not documented as stable |

## Hive

Tables that do not contain an Iceberg or Delta marker are handled as Hive
tables. DobbyDB reads the table location, schema, input format, SerDe
properties, table properties, and partition metadata from HMS or Glue.

### File Formats

| Input format | Status | Notes |
| --- | --- | --- |
| TextFile | Supported | Read through DataFusion's CSV source without a header. |
| Parquet | Supported | Filter pushdown and filter reordering are enabled. |
| ORC | Not supported | The input format is recognized, but query execution returns a not-implemented error. |

For TextFile tables, DobbyDB checks these SerDe properties in order to determine
the field delimiter:

1. `field.delim`
2. `serialization.format`
3. `separatorChar`
4. `serdeConstants.FIELD_DELIM`
5. `columns.delimited.by`

The default delimiter is Hive's control-A byte (`0x01`). A delimiter may be a
single character, an octal escape, `\t`, `\n`, `\r`, a hexadecimal value, a
Unicode-style byte escape, or a decimal byte value.

TextFile compression is detected from file extensions:

| Extensions | Compression |
| --- | --- |
| `.gz`, `.gzip` | Gzip |
| `.bz2` | Bzip2 |
| `.xz` | XZ |
| `.zst`, `.zstd` | Zstandard |
| Other | Uncompressed |

A single scan cannot mix different compression types.

### Partitioning and Metadata

Partitioned tables are supported. DobbyDB loads partition locations and values
from the metastore and applies partition pruning for supported filter
expressions.

The `file_path` metadata table lists visible, non-empty data files:

```sql
SELECT * FROM "table_name$file_path";
```

It returns `file_path` and `file_size`. Files whose names start with `_` or `.`
and zero-byte files are excluded.

### Data Types

| Hive type | DobbyDB / Arrow type |
| --- | --- |
| `tinyint` | `Int8` |
| `smallint` | `Int16` |
| `int`, `integer` | `Int32` |
| `bigint`, `long` | `Int64` |
| `float` | `Float32` |
| `double`, `double precision` | `Float64` |
| `boolean` | `Boolean` |
| `string`, `binary string` | `Utf8` |
| `varchar(...)`, `char(...)` | `Utf8` |
| `binary` | `Binary` |
| `date` | `Date32` |
| `timestamp` | Microsecond timestamp without a timezone |
| `decimal(p,s)` | `Decimal128(p,s)` |
| `decimal` or an unparseable decimal declaration | `Decimal128(38,10)` |

Types not listed above, including Hive complex types, are not currently
supported.

## Iceberg

An HMS or Glue table is treated as Iceberg when its properties contain
`metadata_location`. DobbyDB loads the table directly from that metadata file.

| Capability | Status |
| --- | --- |
| Table reads | Supported |
| Filter pushdown | Delegated to the Iceberg DataFusion provider |
| Partition schema | Supported; non-void partition source columns are exposed by `SHOW CREATE TABLE` |
| Snapshot metadata | Supported through `$snapshots` |
| Manifest metadata | Supported through `$manifests` |
| File-path metadata | Not supported |
| Metadata storage | `file://`, `s3://`, `s3a://`, and `oss://` |

Metadata tables use a `$` suffix. Quote the table name so the SQL parser treats
it as one identifier:

```sql
SELECT * FROM "table_name$snapshots";
SELECT * FROM "table_name$manifests";
```

Iceberg schemas are converted through the Iceberg Arrow schema converter.
DobbyDB therefore follows the type support of the pinned Iceberg and Arrow
implementations rather than maintaining a separate Hive-style type mapping.

## Delta Lake

Delta Lake integration exists in the source tree, and tables are detected when
`spark.sql.sources.provider` is exactly `DELTA`. However, Delta Lake is not yet
perfectly supported and should be treated as incomplete.

The current documentation does not make compatibility guarantees for Delta
Lake reads, writes, storage backends, table features, or data types.
