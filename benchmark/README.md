# Benchmark

This directory contains benchmark SQL files and the Python benchmark runner.

Currently supported benchmark suite:

- `tpch`

Currently supported engines:

- `dobbydb`
- `starrocks`

## Setup

Install Python dependencies when running StarRocks benchmarks:

```bash
python3 -m pip install -r benchmark/requirements.txt
```

DobbyDB benchmarks do not require the Python dependencies because the runner
executes the DobbyDB binary directly.

## Common Options

The runner requires these options for all engines:

- `--engine`: benchmark engine, such as `dobbydb` or `starrocks`.
- `--benchmark-type`: benchmark suite. Currently only `tpch` is supported.
- `--default-catalog`: default catalog for the benchmark session.
- `--default-schema`: default schema or database for the benchmark session.

Optional common options:

- `--runs`: runs per query. Defaults to `3`.
- `--query`: run only one query, such as `q01`. This option can be specified
  multiple times.

## DobbyDB

Run one query:

```bash
python3 benchmark/run_benchmark.py \
  --engine dobbydb \
  --benchmark-type tpch \
  --default-catalog hms_catalog \
  --default-schema tpch_hive_sf1 \
  --runs 1 \
  --query q01 \
  --bin target/debug/dobbydb \
  --config config.toml
```

Run the full TPC-H suite:

```bash
python3 benchmark/run_benchmark.py \
  --engine dobbydb \
  --benchmark-type tpch \
  --default-catalog hms_catalog \
  --default-schema tpch_hive_sf1 \
  --runs 1 \
  --bin target/debug/dobbydb \
  --config config.toml
```

The DobbyDB runner executes each SQL file with `--file` and parses elapsed time
from DobbyDB output.

## StarRocks

Run one query:

```bash
python3 benchmark/run_benchmark.py \
  --engine starrocks \
  --benchmark-type tpch \
  --default-catalog default_catalog \
  --default-schema tpch_hive_sf1 \
  --runs 1 \
  --query q01 \
  --host 127.0.0.1 \
  --port 9030 \
  --user root
```

Use an environment variable when a password is required:

```bash
STARROCKS_PASSWORD='your-password' \
python3 benchmark/run_benchmark.py \
  --engine starrocks \
  --benchmark-type tpch \
  --default-catalog default_catalog \
  --default-schema tpch_hive_sf1 \
  --host 127.0.0.1 \
  --user root \
  --password "$STARROCKS_PASSWORD"
```

The StarRocks runner connects through the MySQL-compatible protocol and runs:

```sql
USE `<default-catalog>`.`<default-schema>`;
```

before executing benchmark SQL files.

## Output

The runner prints one row per query and a summary:

```text
query    |    run1(s) |     avg(s)
---------+------------+-----------
Running q01...
q01      |      2.862 |      2.862

Total queries: 1
Total runs: 1
Total elapsed(s): 2.862
Average per run(s): 2.862
```
