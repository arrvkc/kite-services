#!/usr/bin/env bash
set -euo pipefail

cd /opt/kite_services

set -a
. /opt/kite_services/.env
set +a

[[ $# -eq 0 || ( $# -eq 1 && "$1" = "--report-only" ) ]] || exit 64
if [[ "${1:-}" != "--report-only" ]]; then
  bash /opt/kite_services/scripts/run_signal_research_daily_db.sh
fi

/opt/kite_services/venv/bin/python \
  /opt/kite_services/scripts/send_signal_research_email_report.py
