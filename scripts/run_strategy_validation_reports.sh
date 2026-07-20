#!/usr/bin/env bash
set -euo pipefail

cd /opt/kite_services

set -a
. /opt/kite_services/.env
set +a

KITE_SERVICES_BASE_DIR="/opt/kite_services" \
  source /opt/kite_services/scripts/runtime/configure_host_database_runtime.sh

PYTHONPATH=.:services /opt/kite_services/venv/bin/python \
  engines/strategy_validation_engine/scripts/generate_fo_universe_validation_csv.py

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
