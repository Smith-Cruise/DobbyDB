#!/usr/bin/env python3
import argparse
import os
import re
import subprocess
import sys
import time
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Sequence


RUNS_PER_QUERY = 3
SUPPORTED_BENCHMARK_TYPES = ("tpch",)
ELAPSED_PATTERN = re.compile(r"^Elapsed ([0-9.]+) seconds\.$", re.MULTILINE)


class BenchmarkError(Exception):
    pass


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
    def run_query(self, sql_file: Path) -> float:
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

    def run_query(self, sql_file: Path) -> float:
        sql_path_for_cli = Path("benchmark") / self.benchmark_type / sql_file.name
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
        if result.returncode != 0:
            raise BenchmarkError(result.stdout.rstrip())

        elapsed_seconds = parse_elapsed_seconds(result.stdout)
        if elapsed_seconds is None:
            raise BenchmarkError(
                "failed to parse elapsed time from DobbyDB output:\n"
                f"{result.stdout.rstrip()}"
            )
        return elapsed_seconds


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

    def run_query(self, sql_file: Path) -> float:
        if self.connection is None:
            raise BenchmarkError("StarRocks connection has not been prepared")

        sql = sql_file.read_text(encoding="utf-8")
        start = time.perf_counter()
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(sql)
                while True:
                    cursor.fetchall()
                    if not cursor.nextset():
                        break
        except Exception as exc:
            raise BenchmarkError(f"StarRocks query failed: {exc}") from exc
        return time.perf_counter() - start

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
    return total_seconds, total_seconds / len(times)


def print_results_header(runs: int) -> None:
    headers = ["query", *(f"run{index}(s)" for index in range(1, runs + 1)), "avg(s)"]
    widths = [8, *(10 for _ in range(runs)), 10]
    print_row(headers, widths)
    print_separator(widths)


def print_row(values: Sequence[str], widths: Sequence[int]) -> None:
    formatted = [
        f"{value:<{width}}" if index == 0 else f"{value:>{width}}"
        for index, (value, width) in enumerate(zip(values, widths))
    ]
    print(" | ".join(formatted))


def print_separator(widths: Sequence[int]) -> None:
    print("-+-".join("-" * width for width in widths))


def run_benchmark(args: argparse.Namespace, repo_root: Path) -> None:
    suite = BenchmarkSuite(repo_root, args.benchmark_type, args.queries)
    runner = build_runner(args, repo_root)
    all_times = []

    try:
        runner.prepare()
        print_results_header(args.runs)

        for sql_file in suite.sql_files:
            query_name = sql_file.stem
            run_values = []

            print(f"Running {query_name}...")
            for run_index in range(1, args.runs + 1):
                try:
                    elapsed_seconds = runner.run_query(sql_file)
                except BenchmarkError as exc:
                    raise BenchmarkError(
                        f"benchmark failed for {query_name} on run {run_index}: {exc}"
                    ) from exc
                run_values.append(elapsed_seconds)
                all_times.append(elapsed_seconds)

            average_seconds = sum(run_values) / len(run_values)
            row = [
                query_name,
                *(format_seconds(value) for value in run_values),
                format_seconds(average_seconds),
            ]
            print_row(row, [8, *(10 for _ in range(args.runs)), 10])
    finally:
        runner.close()

    total_seconds, overall_average_seconds = summarize_times(all_times)

    print()
    print(f"Total queries: {len(suite.sql_files)}")
    print(f"Total runs: {len(all_times)}")
    print(f"Total elapsed(s): {format_seconds(total_seconds)}")
    print(f"Average per run(s): {format_seconds(overall_average_seconds)}")


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
