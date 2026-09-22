#!/usr/bin/env bash
set -euo pipefail

cd /opt/kite_services

set -a
source /opt/kite_services/.env
set +a

KITE_SERVICES_BASE_DIR="/opt/kite_services" \
  source /opt/kite_services/scripts/runtime/configure_host_database_runtime.sh

RUN_DATE=$(date +%F)
REPORT_ONLY=0
DATA_ONLY=0
case "${1:-}" in
  --report-only) [[ $# -eq 1 ]] || exit 64; REPORT_ONLY=1 ;;
  --run-date)
    [[ $# -eq 2 ]] || exit 64
    RUN_DATE="$2"
    /opt/kite_services/venv/bin/python -c 'from datetime import date; import sys; date.fromisoformat(sys.argv[1])' "$RUN_DATE"
    DATA_ONLY=1 ;;
  "") ;;
  *) exit 64 ;;
esac

if (( REPORT_ONLY == 0 )); then

/opt/kite_services/venv/bin/python /opt/kite_services/engines/strategy_deterministic_engine/scripts/generate_transition_report_from_db.py \
  --run-date "$RUN_DATE" \
  --user-id "${EAJEE_STRATEGY_USER_ID:-XJ1877}"

bash /opt/kite_services/scripts/publish_transition_dashboard_archive.sh
fi

if (( DATA_ONLY == 0 )); then

/opt/kite_services/venv/bin/python \
  /opt/kite_services/scripts/send_transition_dashboard_email.py
fi

echo "Transition dashboard pipeline completed for $RUN_DATE"
