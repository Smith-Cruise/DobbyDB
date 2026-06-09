# Table Format

DobbyDB determines a table format from metastore table properties. Table
formats are discovered through HMS or Glue.

## Hive

### Supported File Formats

| Input format | Status
| --- | --- |
| TextFile | Supported
| Parquet | Supported
| ORC | Not supported

### Data Types

| Hive type | Arrow type |
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

### Metadata Table

**file_path**

The `file_path` metadata table lists visible, non-empty data files:

```sql
SELECT * FROM "table_name$file_path";
```

It returns `file_path` and `file_size`. Files whose names start with `_` or `.`
and zero-byte files are excluded.

## Iceberg

An HMS or Glue table is treated as Iceberg when its properties contain
`metadata_location`. DobbyDB loads the table directly from that metadata file.

Data file format only support *Parquet* now.

### Data Types

The same as `iceberg-rust`.

### Metadata Table

**snapshots**

```sql
SELECT * FROM `table_name$snapshots`;
```

**manifests**

```sql
SELECT * FROM `table_name$manifests`;
```

## Delta Lake

Delta Lake integration exists in the source tree, and tables are detected when
`spark.sql.sources.provider` is exactly `DELTA`. However, Delta Lake is not yet
perfectly supported and should be treated as incomplete.