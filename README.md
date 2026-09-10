# Lakelet

**Wake up, query the data lake.**

Lakelet is a single-binary SQL engine for lakehouse tables. No Spark cluster is
needed, query delta lake tables directly with SQL.

```sql
show catalogs;
use catalog hms;
select * from default.orders limit 10;
```

## Features

| Area | Support |
| --- | --- |
| Catalog | Hive Metastore, AWS Glue, Paimon filesystem, Iceberg REST |
| Table format | Iceberg, Hive, Paimon, Delta Lake |
| File format | Parquet, Hive TextFile |
| Storage | S3 / S3-compatible storage, Aliyun OSS, HDFS |

## Quick Start

Install the latest nightly binary into the current directory (Linux and macOS;
Windows builds are on the [releases page](https://github.com/Smith-Cruise/Lakelet/releases)):

```bash
curl -fsSL https://lakelet.dev/install.sh | sh
```

Or build from source:

```bash
cargo build --release
cp target/release/lakelet .
cp config_demo.toml config.toml
```

Configure at least one external catalog in
`config.toml`:

```toml
[[catalog.hms]]
name = "hms"
metastore-uri = "127.0.0.1:9083"
s3-storage = { region = "us-east-1", endpoint = "http://127.0.0.1:9000", access-key = "admin", secret-key = "password", path-style-access = true }
```

Start the SQL shell:

```bash
./lakelet --config config.toml
```

Run one SQL command and exit:

```bash
./lakelet --config config.toml --command "show catalogs;"
```

Run SQL from a file:

```bash
./lakelet --config config.toml --file query.sql
```

## Basic SQL

```sql
show catalogs;
use catalog hms;
show schemas;
use default;
show tables;
select * from my_table limit 10;
```

You can also use fully qualified table names:

```sql
select * from hms.default.my_table limit 10;
```

## Documentation

https://lakelet.dev/
