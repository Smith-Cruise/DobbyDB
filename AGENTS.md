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
Project's code comments, pull request must use English.

Pass format check:
```bash
cargo fmt --all -- --check
```

Pass clippy check:
```bash
cargo clippy --all-targets --all-features -- -D warnings
```

Pass tests:
```bash
cargo test --all-targets --all-features --verbose
```

## Pull Request Requirements
* PR title and descriptions must use English.
* Keep the PR descriptions as concise as possible.