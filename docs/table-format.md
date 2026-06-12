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

**data_files**

The `data_files` metadata table lists visible, non-empty data files:

```sql
SELECT * FROM `table_name$data_files`;
```

It returns `file_path` and `file_size`. Files whose names start with `_` or `.`
and zero-byte files are excluded.

**partitions**

The `partitions` metadata table returns one row for each metastore partition:

```sql
SELECT * FROM `table_name$partitions`;
```

It returns `partition`, `data_file_count`, and `total_data_file_size`.
`partition` is a string such as `dt=2026-01-01/country=CN`.
An unpartitioned table returns no rows.

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

## Paimon

An HMS or Glue table is treated as Paimon when its storage descriptor input
format is exactly:

```text
org.apache.paimon.hive.mapred.PaimonInputFormat
```

DobbyDB loads the latest Paimon schema from the table location and uses
`paimon-rust` for DataFusion scans. Column projection and filter pushdown are
provided by the upstream Paimon DataFusion integration.

Paimon tables are currently read-only. Inserts, overwrites, DDL, time travel,
and Paimon system tables such as `$snapshots` are not supported.

Supported storage schemes are `s3://`, `s3a://`, `oss://`, and `hdfs://`.
S3/S3A and OSS reuse the storage credentials configured on the containing HMS
or Glue catalog. HDFS obtains the NameNode authority from the table location.
