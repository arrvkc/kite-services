#!/usr/bin/env bash
set -euo pipefail

cd /opt/kite_services

set -a
source /opt/kite_services/.env
set +a

KITE_SERVICES_BASE_DIR="/opt/kite_services" \
  source /opt/kite_services/scripts/runtime/configure_host_database_runtime.sh

RUN_DATE=$(date +%F)

/opt/kite_services/venv/bin/python /opt/kite_services/engines/strategy_deterministic_engine/scripts/generate_transition_report_from_db.py \
  --run-date "$RUN_DATE" \
  --user-id OMK569

bash /opt/kite_services/scripts/publish_transition_dashboard_archive.sh

/opt/kite_services/venv/bin/python \
  /opt/kite_services/scripts/send_transition_dashboard_email.py

echo "Transition dashboard pipeline completed for $RUN_DATE"
