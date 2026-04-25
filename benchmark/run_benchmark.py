#!/usr/bin/env python3
import argparse
import csv
import os
import re
import subprocess
import sys
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Sequence


RUNS_PER_QUERY = 1
SUPPORTED_BENCHMARK_TYPES = ("tpch",)
ELAPSED_PATTERN = re.compile(r"^Elapsed ([0-9.]+) seconds\.$", re.MULTILINE)


class BenchmarkError(Exception):
    def __init__(self, message: str, raw_output: str | None = None) -> None:
        super().__init__(message)
        self.raw_output = raw_output


@dataclass(frozen=True)
class QueryRunResult:
    elapsed_seconds: float
    raw_output: str


class BenchmarkOutput:
    def __init__(self, run_dir: Path) -> None:
        self.run_dir = run_dir
        self.raw_dir = run_dir / "raw"
        self.console_path = run_dir / "console.txt"
        self.results_path = run_dir / "results.csv"

        self.raw_dir.mkdir(parents=True, exist_ok=False)
        self.console_file = self.console_path.open("w", encoding="utf-8")
        self.results_file = self.results_path.open("w", newline="", encoding="utf-8")
        self.results_writer = csv.writer(self.results_file)
        self.results_writer.writerow(["query", "run", "status", "elapsed_seconds", "error"])
        self.results_file.flush()

    @classmethod
    def create(cls, output_root: Path) -> "BenchmarkOutput":
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        output_root = output_root.expanduser()
        if not output_root.is_absolute():
            output_root = output_root.resolve()
        run_dir = output_root / f"dobbydb-benchmark-{timestamp}-{os.getpid()}"
        run_dir.mkdir(parents=True, exist_ok=False)
        return cls(run_dir)

    def log(self, message: str = "") -> None:
        print(message)
        print(message, file=self.console_file)
        self.console_file.flush()

    def write_result(self, query_name: str, run_index: int, result: QueryRunResult) -> None:
        self.results_writer.writerow(
            [query_name, run_index, "success", format_seconds(result.elapsed_seconds), ""]
        )
        self.results_file.flush()
        self.write_raw_output(query_name, run_index, result.raw_output)

    def write_failure(
        self,
        query_name: str,
        run_index: int,
        error: BenchmarkError,
    ) -> None:
        error_message = " ".join(str(error).splitlines())
        self.results_writer.writerow(
            [query_name, run_index, "failed", "", error_message]
        )
        self.results_file.flush()
        if error.raw_output is not None:
            self.write_raw_output(query_name, run_index, error.raw_output)

    def write_raw_output(self, query_name: str, run_index: int, raw_output: str) -> None:
        query_dir = self.raw_dir / query_name
        query_dir.mkdir(parents=True, exist_ok=True)
        raw_path = query_dir / f"run{run_index}.txt"
        raw_path.write_text(raw_output, encoding="utf-8")

    def close(self) -> None:
        self.results_file.close()
        self.console_file.close()


class BenchmarkSuite:
    def __init__(
        self, repo_root: Path, benchmark_type: str, query_names: Sequence[str] | None
    ) -> None:
        if benchmark_type not in SUPPORTED_BENCHMARK_TYPES:
            supported = ", ".join(SUPPORTED_BENCHMARK_TYPES)
            raise BenchmarkError(
                f"unsupported benchmark type: {benchmark_type}. Supported values: {supported}"
            )

        self.benchmark_type = benchmark_type
        self.sql_dir = repo_root / "benchmark" / benchmark_type
        if not self.sql_dir.is_dir():
            raise BenchmarkError(f"benchmark SQL directory does not exist: {self.sql_dir}")

        self.sql_files = sorted(self.sql_dir.glob("q*.sql"))
        if not self.sql_files:
            raise BenchmarkError(f"no SQL files found under {self.sql_dir}")

        if query_names:
            requested = {normalize_query_name(query_name) for query_name in query_names}
            sql_files_by_name = {sql_file.stem: sql_file for sql_file in self.sql_files}
            missing = sorted(requested.difference(sql_files_by_name))
            if missing:
                raise BenchmarkError(
                    f"benchmark SQL files do not exist: {', '.join(missing)}"
                )
            self.sql_files = [sql_files_by_name[query_name] for query_name in sorted(requested)]


class EngineRunner(ABC):
    def prepare(self) -> None:
        pass

    @abstractmethod
    def run_query(self, sql_file: Path) -> QueryRunResult:
        pass

    def close(self) -> None:
        pass


class DobbyDbRunner(EngineRunner):
    def __init__(
        self,
        repo_root: Path,
        benchmark_type: str,
        bin_path: Path,
        config_path: Path,
        default_catalog: str,
        default_schema: str,
    ) -> None:
        self.repo_root = repo_root
        self.benchmark_type = benchmark_type
        self.bin_path = bin_path
        self.config_path = config_path
        self.default_catalog = default_catalog
        self.default_schema = default_schema

        if not self.bin_path.is_file() or not os.access(self.bin_path, os.X_OK):
            raise BenchmarkError(f"binary is not executable: {self.bin_path}")
        if not self.config_path.is_file():
            raise BenchmarkError(f"config file does not exist: {self.config_path}")

    def run_query(self, sql_file: Path) -> QueryRunResult:
        sql_path_for_cli = Path("benchmark") / self.benchmark_type / sql_file.name
        sql = sql_file.read_text(encoding="utf-8")
        command = [
            str(self.bin_path),
            "--config",
            str(self.config_path),
            "--default-catalog",
            self.default_catalog,
            "--default-schema",
            self.default_schema,
            "--file",
            str(sql_path_for_cli),
        ]

        result = subprocess.run(
            command,
            cwd=self.repo_root,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            check=False,
        )
        raw_output = format_dobbydb_output(command, sql, result.stdout)
        if result.returncode != 0:
            raise BenchmarkError(result.stdout.rstrip(), raw_output=raw_output)

        elapsed_seconds = parse_elapsed_seconds(result.stdout)
        if elapsed_seconds is None:
            raise BenchmarkError(
                "failed to parse elapsed time from DobbyDB output:\n"
                f"{result.stdout.rstrip()}",
                raw_output=raw_output,
            )
        return QueryRunResult(elapsed_seconds=elapsed_seconds, raw_output=raw_output)


class StarRocksRunner(EngineRunner):
    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: str | None,
        default_catalog: str,
        default_schema: str,
    ) -> None:
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.default_catalog = default_catalog
        self.default_schema = default_schema
        self.connection = None

    def prepare(self) -> None:
        try:
            import pymysql
        except ImportError as exc:
            raise BenchmarkError(
                "PyMySQL is required for StarRocks benchmarks. "
                "Install it with: pip install -r benchmark/requirements.txt"
            ) from exc

        try:
            self.connection = pymysql.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password or "",
                autocommit=True,
            )
            with self.connection.cursor() as cursor:
                catalog = quote_identifier(self.default_catalog)
                schema = quote_identifier(self.default_schema)
                cursor.execute(f"USE {catalog}.{schema}")
        except Exception as exc:
            self.close()
            raise BenchmarkError(f"failed to connect to StarRocks: {exc}") from exc

    def run_query(self, sql_file: Path) -> QueryRunResult:
        if self.connection is None:
            raise BenchmarkError("StarRocks connection has not been prepared")

        sql = sql_file.read_text(encoding="utf-8")
        result_sets = []
        start = time.perf_counter()
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(sql)
                while True:
                    columns = (
                        [description[0] for description in cursor.description]
                        if cursor.description
                        else []
                    )
                    rows = cursor.fetchall()
                    result_sets.append((columns, rows))
                    if not cursor.nextset():
                        break
        except Exception as exc:
            raw_output = format_starrocks_output(
                query_name=sql_file.stem,
                sql=sql,
                status="failed",
                elapsed_seconds=None,
                result_sets=result_sets,
                error=str(exc),
            )
            raise BenchmarkError(
                f"StarRocks query failed: {exc}", raw_output=raw_output
            ) from exc

        elapsed_seconds = time.perf_counter() - start
        raw_output = format_starrocks_output(
            query_name=sql_file.stem,
            sql=sql,
            status="success",
            elapsed_seconds=elapsed_seconds,
            result_sets=result_sets,
            error=None,
        )
        return QueryRunResult(elapsed_seconds=elapsed_seconds, raw_output=raw_output)

    def close(self) -> None:
        if self.connection is not None:
            self.connection.close()
            self.connection = None


def parse_elapsed_seconds(output: str) -> float | None:
    match = ELAPSED_PATTERN.search(output)
    if match is None:
        return None
    return float(match.group(1))


def quote_identifier(identifier: str) -> str:
    escaped = identifier.replace("`", "``")
    return f"`{escaped}`"


def format_dobbydb_output(command: Sequence[str], sql: str, process_output: str) -> str:
    sql_result = extract_dobbydb_sql_result(process_output)
    return "\n".join(
        [
            "command:",
            " ".join(command),
            "",
            "sql:",
            sql.rstrip(),
            "",
            "sql_result:",
            sql_result,
            "",
            "raw_output:",
            process_output.rstrip(),
            "",
        ]
    )


def extract_dobbydb_sql_result(process_output: str) -> str:
    result_lines = []
    for line in process_output.splitlines():
        if line.startswith("Elapsed ") or re.match(r"^\d+ row\(s\) fetched\.", line):
            break
        result_lines.append(line)

    result = "\n".join(result_lines).strip()
    if result:
        return result
    return "(no SQL result captured from DobbyDB stdout)"


def format_starrocks_output(
    query_name: str,
    sql: str,
    status: str,
    elapsed_seconds: float | None,
    result_sets: Sequence[tuple[Sequence[str], Sequence[Sequence[object]]]],
    error: str | None,
) -> str:
    lines = [
        f"query={query_name}",
        f"status={status}",
    ]
    if elapsed_seconds is not None:
        lines.append(f"elapsed_seconds={format_seconds(elapsed_seconds)}")
    if error is not None:
        lines.append(f"error={error}")

    lines.extend(["", "sql:", sql.rstrip(), "", "output:"])
    if not result_sets:
        lines.append("(no result sets)")
    for result_set_index, (columns, rows) in enumerate(result_sets, start=1):
        lines.append(f"result_set={result_set_index}")
        lines.append(format_result_set(columns, rows))
    lines.append("")
    return "\n".join(lines)


def format_result_set(
    columns: Sequence[str], rows: Sequence[Sequence[object]]
) -> str:
    if not columns:
        return f"(no columns, {len(rows)} rows)"

    table_rows = [[format_sql_value(value) for value in row] for row in rows]
    widths = [
        max(len(str(column)), *(len(row[index]) for row in table_rows))
        for index, column in enumerate(columns)
    ]
    header = " | ".join(
        f"{column:<{width}}" for column, width in zip(columns, widths)
    )
    separator = "-+-".join("-" * width for width in widths)
    body = [
        " | ".join(f"{value:<{width}}" for value, width in zip(row, widths))
        for row in table_rows
    ]
    return "\n".join([header, separator, *body, f"({len(rows)} rows)"])


def format_sql_value(value: object) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bytes):
        return value.hex()
    return str(value)


def positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid positive integer: {value}") from exc

    if parsed <= 0:
        raise argparse.ArgumentTypeError(f"invalid positive integer: {value}")
    return parsed


def normalize_query_name(query_name: str) -> str:
    query_name = query_name.strip()
    if query_name.endswith(".sql"):
        query_name = query_name[:-4]
    if not query_name:
        raise argparse.ArgumentTypeError("query name must not be empty")
    return query_name


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run DobbyDB and AP database benchmarks.",
    )
    parser.add_argument(
        "--engine",
        choices=("dobbydb", "starrocks"),
        required=True,
        help="Benchmark engine to run.",
    )
    parser.add_argument(
        "--benchmark-type",
        choices=SUPPORTED_BENCHMARK_TYPES,
        required=True,
        help="Benchmark type to run.",
    )
    parser.add_argument(
        "--default-catalog",
        required=True,
        help="Default catalog name for the benchmark session.",
    )
    parser.add_argument(
        "--default-schema",
        required=True,
        help="Default schema/database name for the benchmark session.",
    )
    parser.add_argument(
        "--runs",
        type=positive_int,
        default=RUNS_PER_QUERY,
        help=f"Runs per query. Defaults to {RUNS_PER_QUERY}.",
    )
    parser.add_argument(
        "--query",
        action="append",
        type=normalize_query_name,
        dest="queries",
        help="Run only the named query, for example q01. Can be specified multiple times.",
    )
    parser.add_argument(
        "--output",
        default="/tmp",
        help="Directory for benchmark output artifacts. Defaults to /tmp.",
    )

    dobbydb = parser.add_argument_group("DobbyDB connection")
    dobbydb.add_argument("--bin", help="Path to the DobbyDB binary.")
    dobbydb.add_argument("--config", help="Path to the DobbyDB config file.")

    starrocks = parser.add_argument_group("StarRocks connection")
    starrocks.add_argument("--host", help="StarRocks host.")
    starrocks.add_argument(
        "--port",
        type=positive_int,
        default=9030,
        help="StarRocks MySQL-compatible query port. Defaults to 9030.",
    )
    starrocks.add_argument("--user", help="StarRocks user.")
    starrocks.add_argument(
        "--password",
        help="StarRocks password. Defaults to STARROCKS_PASSWORD when omitted.",
    )

    return parser


def validate_args(parser: argparse.ArgumentParser, args: argparse.Namespace) -> None:
    if args.engine == "dobbydb":
        if not args.bin:
            parser.error("--bin is required when --engine dobbydb")
        if not args.config:
            parser.error("--config is required when --engine dobbydb")
    elif args.engine == "starrocks":
        if not args.host:
            parser.error("--host is required when --engine starrocks")
        if not args.user:
            parser.error("--user is required when --engine starrocks")


def build_runner(args: argparse.Namespace, repo_root: Path) -> EngineRunner:
    if args.engine == "dobbydb":
        return DobbyDbRunner(
            repo_root=repo_root,
            benchmark_type=args.benchmark_type,
            bin_path=Path(args.bin).expanduser().resolve(),
            config_path=Path(args.config).expanduser().resolve(),
            default_catalog=args.default_catalog,
            default_schema=args.default_schema,
        )

    password = args.password
    if password is None:
        password = os.environ.get("STARROCKS_PASSWORD")

    return StarRocksRunner(
        host=args.host,
        port=args.port,
        user=args.user,
        password=password,
        default_catalog=args.default_catalog,
        default_schema=args.default_schema,
    )


def format_seconds(value: float) -> str:
    return f"{value:.3f}"


def summarize_times(times: Sequence[float]) -> tuple[float, float]:
    total_seconds = sum(times)
    average_seconds = total_seconds / len(times) if times else 0.0
    return total_seconds, average_seconds


def print_row(values: Sequence[str], widths: Sequence[int]) -> str:
    formatted = [
        f"{value:<{width}}" if index == 0 else f"{value:>{width}}"
        for index, (value, width) in enumerate(zip(values, widths))
    ]
    return " | ".join(formatted)


def print_separator(widths: Sequence[int]) -> str:
    return "-+-".join("-" * width for width in widths)


def log_results_header(output: BenchmarkOutput, runs: int) -> None:
    headers = ["query", *(f"run{index}(s)" for index in range(1, runs + 1)), "avg(s)"]
    widths = [8, *(10 for _ in range(runs)), 10]
    output.log(print_row(headers, widths))
    output.log(print_separator(widths))


def run_benchmark(args: argparse.Namespace, repo_root: Path) -> None:
    output = BenchmarkOutput.create(Path(args.output))
    runner = None

    try:
        suite = BenchmarkSuite(repo_root, args.benchmark_type, args.queries)
        runner = build_runner(args, repo_root)
        all_times = []
        failed_runs = 0

        try:
            runner.prepare()
            log_results_header(output, args.runs)

            for sql_file in suite.sql_files:
                query_name = sql_file.stem
                run_values = []
                run_cells = []

                output.log(f"Running {query_name}...")
                for run_index in range(1, args.runs + 1):
                    try:
                        result = runner.run_query(sql_file)
                    except BenchmarkError as exc:
                        failed_runs += 1
                        output.write_failure(query_name, run_index, exc)
                        output.log(
                            f"Skip {query_name} run {run_index}: benchmark failed: {exc}"
                        )
                        run_cells.append("FAILED")
                        continue
                    output.write_result(query_name, run_index, result)
                    run_values.append(result.elapsed_seconds)
                    run_cells.append(format_seconds(result.elapsed_seconds))
                    all_times.append(result.elapsed_seconds)

                average_seconds = sum(run_values) / len(run_values) if run_values else None
                row = [
                    query_name,
                    *run_cells,
                    "FAILED" if average_seconds is None else format_seconds(average_seconds),
                ]
                output.log(print_row(row, [8, *(10 for _ in range(args.runs)), 10]))
        finally:
            if runner is not None:
                runner.close()

        total_seconds, overall_average_seconds = summarize_times(all_times)

        output.log()
        output.log(f"Total queries: {len(suite.sql_files)}")
        output.log(f"Successful runs: {len(all_times)}")
        output.log(f"Failed runs: {failed_runs}")
        output.log(f"Total elapsed(s): {format_seconds(total_seconds)}")
        output.log(f"Average per run(s): {format_seconds(overall_average_seconds)}")
        output.log(f"Benchmark output: {output.run_dir}")
    except BenchmarkError:
        output.log()
        output.log(f"Benchmark output: {output.run_dir}")
        raise
    finally:
        output.close()


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    validate_args(parser, args)

    repo_root = Path(__file__).resolve().parent.parent
    try:
        run_benchmark(args, repo_root)
    except BenchmarkError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
