#!/usr/bin/env bash
set -euo pipefail

THROUGH_DATE=""
if [[ $# -gt 0 ]]; then
    if [[ $# -ne 2 || "$1" != "--through-date" ]]; then
        echo "Usage: $0 [--through-date YYYY-MM-DD]" >&2
        exit 64
    fi
    THROUGH_DATE="$2"
    python3 -c 'from datetime import date; import sys; date.fromisoformat(sys.argv[1])' \
        "$THROUGH_DATE" >/dev/null 2>&1 || {
        echo "Invalid --through-date: $THROUGH_DATE" >&2
        exit 64
    }
fi

cd /opt/kite_services

set -a
. /opt/kite_services/.env
set +a

KITE_SERVICES_BASE_DIR="/opt/kite_services" \
  source /opt/kite_services/scripts/runtime/configure_host_database_runtime.sh

VALIDATION_DATE_ARGS=()
if [[ -n "$THROUGH_DATE" ]]; then
    VALIDATION_DATE_ARGS=(--through-date "$THROUGH_DATE")
fi

PYTHONPATH=.:services /opt/kite_services/venv/bin/python \
  engines/strategy_validation_engine/scripts/generate_fo_universe_validation_csv.py \
  "${VALIDATION_DATE_ARGS[@]}"

PYTHONPATH=.:services /opt/kite_services/venv/bin/python \
  engines/strategy_validation_engine/scripts/generate_directional_accuracy_report.py

PYTHONPATH=.:services /opt/kite_services/venv/bin/python \
  engines/strategy_validation_engine/scripts/generate_directional_accuracy_summary.py

PYTHONPATH=.:services /opt/kite_services/venv/bin/python \
  engines/strategy_validation_engine/scripts/generate_strength_lift_report.py

PYTHONPATH=.:services /opt/kite_services/venv/bin/python \
  engines/strategy_validation_engine/scripts/generate_validation_dashboard_split.py

PYTHONPATH=.:services /opt/kite_services/venv/bin/python \
engines/strategy_validation_engine/scripts/generate_validation_dashboard_payload.py

ATMS_INSTANCE_HOST_PATH="${ATMS_INSTANCE_HOST_PATH:-/srv/atms-platform/instance}"
VALIDATION_DEST_DIR="$ATMS_INSTANCE_HOST_PATH/strategy_validation"

mkdir -p "$VALIDATION_DEST_DIR"

install -m 0644 \
/opt/kite_services/data/strategy_validation/validation_dashboard_payload.json \
"$VALIDATION_DEST_DIR/validation_dashboard_payload.json"

echo "Strategy validation reports completed. through_date=${THROUGH_DATE:-ALL}"
