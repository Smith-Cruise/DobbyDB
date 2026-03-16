## Project Description
DobbyDB is a datafusion based query engine. It focuses on lakehouse query.

## Supported Table Format
* Iceberg (parquet)
* Delta Lake (parquet)
* Hive (textfile, parquet)

## Supported Metastore
* HMS
* Glue

## Commit Requirements
Format check:
```bash
cargo fmt --all -- --check
```

Clippy check:
```bash
cargo clippy --all-targets --all-features -- -D warnings
```

Pass tests:
```bash
cargo test --all-targets --all-features --verbose
```
