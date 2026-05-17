## Project Description
DobbyDB is a datafusion based query engine. It focuses on lakehouse query.

### Supported Table Format
* Iceberg (parquet)
* Delta Lake (parquet)
* Hive (textfile, parquet)

### Supported Metastore
* HMS
* Glue

## Agents rules
Agents should follow below rules:

* Reply based on the language used in the user's question.
* Code's comments, commit message, pull request must use English.
* Keep the PR descriptions as concise as possible.

### Pull request checklist
Agents should finish below checklist before pull request.

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
