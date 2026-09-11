#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
COMPOSE_FILE="${REPO_ROOT}/integration-tests/docker-compose.yml"
KEEP_COMPOSE=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --keep-compose)
      KEEP_COMPOSE=1
      shift
      ;;
    *)
      echo "Unknown argument: $1" >&2
      echo "Usage: $0 [--keep-compose]" >&2
      exit 1
      ;;
  esac
done

cd "${REPO_ROOT}"

cleanup() {
  if [[ "${KEEP_COMPOSE}" != "1" ]]; then
    docker compose -f "${COMPOSE_FILE}" down -v --remove-orphans
  else
    echo "--keep-compose set, leaving integration test containers running"
  fi
}
trap cleanup EXIT

python3 -m pip install -r integration-tests/requirements.txt
integration-tests/download-jars.sh

docker compose -f "${COMPOSE_FILE}" down -v --remove-orphans

# 5050: moto container; 8181: the Iceberg REST catalog container;
# 32010: the Flight SQL server pytest starts.
for port in 5050 8181 32010; do
  if lsof -nP -iTCP:"${port}" -sTCP:LISTEN >"/tmp/lakelet-integration-port-${port}.txt" 2>/dev/null; then
    echo "Port ${port} is already in use:" >&2
    cat "/tmp/lakelet-integration-port-${port}.txt" >&2
    exit 1
  fi
done

docker compose -f "${COMPOSE_FILE}" up -d --wait
if [[ -n "${LAKELET_BIN:-}" ]]; then
  if [[ ! -x "${LAKELET_BIN}" ]]; then
    echo "LAKELET_BIN is set but not executable: ${LAKELET_BIN}" >&2
    exit 1
  fi
else
  cargo build -p lakelet-app --bin lakelet
  export LAKELET_BIN="${REPO_ROOT}/target/debug/lakelet"
fi
python3 integration-tests/provision.py
python3 -m pytest -s integration-tests
