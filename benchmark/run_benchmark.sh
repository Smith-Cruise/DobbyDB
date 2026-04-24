#!/usr/bin/env bash
set -euo pipefail

readonly RUNS_PER_QUERY=3

usage() {
    cat <<'EOF'
Usage:
  ./run_benchmark.sh \
    --bin <path> \
    --config <path> \
    --default-catalog <name> \
    --default-schema <name> \
    --benchmark-type <type>

Options:
  --bin               Path to the DobbyDB binary.
  --config            Path to the config file.
  --default-catalog   Default catalog name.
  --default-schema    Default schema name.
  --benchmark-type    Benchmark type. Only "tpch" is supported.
  --help              Show this help message.
EOF
}

error() {
    echo "Error: $*" >&2
}

require_value() {
    local option="$1"
    local value="${2:-}"

    if [[ -z "${value}" ]]; then
        error "missing value for ${option}"
        usage
        exit 1
    fi
}

parse_elapsed_seconds() {
    local log_file="$1"
    awk '/^Elapsed [0-9.]+ seconds\.$/ { print $2; exit }' "${log_file}"
}

sum_times() {
    awk 'BEGIN { sum = 0 } { sum += $1 } END { printf "%.3f", sum }'
}

average_times() {
    local count="$1"
    awk -v count="${count}" 'BEGIN { sum = 0 } { sum += $1 } END { printf "%.3f", sum / count }'
}

bin_path=""
config_path=""
default_catalog=""
default_schema=""
benchmark_type=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --bin)
            require_value "$1" "${2:-}"
            bin_path="$2"
            shift 2
            ;;
        --config)
            require_value "$1" "${2:-}"
            config_path="$2"
            shift 2
            ;;
        --default-catalog)
            require_value "$1" "${2:-}"
            default_catalog="$2"
            shift 2
            ;;
        --default-schema)
            require_value "$1" "${2:-}"
            default_schema="$2"
            shift 2
            ;;
        --benchmark-type)
            require_value "$1" "${2:-}"
            benchmark_type="$2"
            shift 2
            ;;
        --help|-h)
            usage
            exit 0
            ;;
        *)
            error "unknown argument: $1"
            usage
            exit 1
            ;;
    esac
done

if [[ -z "${bin_path}" || -z "${config_path}" || -z "${default_catalog}" || -z "${default_schema}" || -z "${benchmark_type}" ]]; then
    error "all required arguments must be provided"
    usage
    exit 1
fi

if [[ "${benchmark_type}" != "tpch" ]]; then
    error "unsupported benchmark type: ${benchmark_type}. Only \"tpch\" is supported."
    exit 1
fi

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/.." && pwd)"
sql_dir="${repo_root}/benchmark/tpch"

if [[ ! -x "${bin_path}" ]]; then
    error "binary is not executable: ${bin_path}"
    exit 1
fi

if [[ ! -f "${config_path}" ]]; then
    error "config file does not exist: ${config_path}"
    exit 1
fi

if [[ ! -d "${sql_dir}" ]]; then
    error "benchmark SQL directory does not exist: ${sql_dir}"
    exit 1
fi

shopt -s nullglob
sql_files=("${sql_dir}"/q*.sql)
shopt -u nullglob

if [[ ${#sql_files[@]} -eq 0 ]]; then
    error "no SQL files found under ${sql_dir}"
    exit 1
fi

tmp_dir="$(mktemp -d)"
cleanup() {
    rm -rf "${tmp_dir}"
}
trap cleanup EXIT

cd "${repo_root}"

printf "%-8s | %10s | %10s | %10s | %10s\n" "query" "run1(s)" "run2(s)" "run3(s)" "avg(s)"
printf "%-8s-+-%10s-+-%10s-+-%10s-+-%10s\n" "--------" "----------" "----------" "----------" "----------"

overall_times_file="${tmp_dir}/overall_times.txt"

for sql_file in "${sql_files[@]}"; do
    query_name="$(basename "${sql_file}" .sql)"
    sql_path_for_cli="benchmark/${benchmark_type}/$(basename "${sql_file}")"
    query_times_file="${tmp_dir}/${query_name}_times.txt"
    run_values=()

    echo "Running ${query_name}..."

    for ((run_index = 1; run_index <= RUNS_PER_QUERY; run_index++)); do
        log_file="${tmp_dir}/${query_name}_run${run_index}.log"

        if ! "${bin_path}" \
            --config "${config_path}" \
            --default-catalog "${default_catalog}" \
            --default-schema "${default_schema}" \
            --file "${sql_path_for_cli}" >"${log_file}" 2>&1; then
            error "benchmark failed for ${query_name} on run ${run_index}"
            cat "${log_file}" >&2
            exit 1
        fi

        elapsed_seconds="$(parse_elapsed_seconds "${log_file}")"
        if [[ -z "${elapsed_seconds}" ]]; then
            error "failed to parse elapsed time for ${query_name} on run ${run_index}"
            cat "${log_file}" >&2
            exit 1
        fi

        run_values+=("${elapsed_seconds}")
        printf '%s\n' "${elapsed_seconds}" >>"${query_times_file}"
        printf '%s\n' "${elapsed_seconds}" >>"${overall_times_file}"
    done

    average_seconds="$(average_times "${RUNS_PER_QUERY}" <"${query_times_file}")"
    printf "%-8s | %10s | %10s | %10s | %10s\n" \
        "${query_name}" \
        "${run_values[0]}" \
        "${run_values[1]}" \
        "${run_values[2]}" \
        "${average_seconds}"
done

total_runs="$(wc -l <"${overall_times_file}" | tr -d ' ')"
total_seconds="$(sum_times <"${overall_times_file}")"
overall_average_seconds="$(average_times "${total_runs}" <"${overall_times_file}")"

printf '\n'
printf "Total queries: %d\n" "${#sql_files[@]}"
printf "Total runs: %d\n" "${total_runs}"
printf "Total elapsed(s): %s\n" "${total_seconds}"
printf "Average per run(s): %s\n" "${overall_average_seconds}"
