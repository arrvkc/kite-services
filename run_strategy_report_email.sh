#!/usr/bin/env bash
set -Eeuo pipefail

readonly APP_DIR="${EAJEE_KITE_SERVICES_DIR:-/opt/kite_services}"
readonly PYTHON_BIN="${EAJEE_KITE_PYTHON:-$APP_DIR/venv/bin/python}"

if [[ $# -ne 1 || ! "$1" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
    echo "Usage: $0 YYYY-MM-DD" >&2
    exit 64
fi
readonly RUN_DATE="$1"

cd "$APP_DIR"
set -a
# shellcheck source=/dev/null
. "$APP_DIR/.env"
set +a
export KITE_SERVICES_BASE_DIR="$APP_DIR"
# shellcheck source=/dev/null
. "$APP_DIR/scripts/runtime/configure_host_database_runtime.sh"

PYTHONPATH=.:services "$PYTHON_BIN" \
    engines/strategy_deterministic_engine/reports/email_strategy_report.py \
    --run-date "$RUN_DATE"
