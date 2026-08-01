#!/usr/bin/env bash
set -euo pipefail

cd /opt/kite_services

set -a
. /opt/kite_services/.env
set +a

bash /opt/kite_services/scripts/run_signal_research_daily_db.sh

/opt/kite_services/venv/bin/python \
  /opt/kite_services/scripts/send_signal_research_email_report.py
