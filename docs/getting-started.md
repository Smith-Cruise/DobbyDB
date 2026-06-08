# Getting Started

DobbyDB reads its configuration from a TOML file. The configuration defines
the query engine's memory limit, catalogs, and the storage credentials used to
read table data.

## Build DobbyDB

DobbyDB requires a Rust toolchain. Build the release binary from the repository
root:

```bash
./build.sh
```

The binary is written to `target/release/dobbydb`.

## Create a Configuration File

Copy the example configuration and edit it for your environment:

```bash
cp config_demo.toml config.toml
```

A configuration file can contain the following top-level sections:

| Section | Required | Description |
| --- | --- | --- |
| `[server]` | No | Server-wide settings, currently limited to the query engine memory limit. |
| `[[catalog.hms]]` | No | A Hive Metastore catalog. Repeat the block to configure multiple HMS catalogs. |
| `[[catalog.glue]]` | No | An AWS Glue catalog. Repeat the block to configure multiple Glue catalogs. |

At least one HMS or Glue catalog is needed to query external tables. The
built-in `internal` catalog is always available, even when no external catalog
is configured.

The following is a minimal HMS configuration:

```toml
[server]
memory-limit = "4GB"

[[catalog.hms]]
name = "hms"
metastore-uri = "127.0.0.1:9083"
```

Add an `s3-storage` or `oss-storage` entry to the catalog when its tables use
object storage and explicit credentials or a custom endpoint are required:

```toml
[[catalog.hms]]
name = "hms"
metastore-uri = "127.0.0.1:9083"
s3-storage = { region = "us-east-1", endpoint = "http://127.0.0.1:9000", access-key = "admin", secret-key = "password", path-style-access = true }
```

See [Server](server.md), [Catalog](catalog.md), and [Storage](storage.md) for
the complete configuration reference.

## Start DobbyDB

Pass the configuration file with `--config`:

```bash
target/release/dobbydb --config config.toml
```

The configuration file is required for normal execution. Configuration keys
use kebab-case, and invalid values for recognized fields cause startup to fail.
