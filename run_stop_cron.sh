#!/bin/bash
set -euo pipefail

APP_DIR="/opt/kite_services"
LOG_DIR="$APP_DIR/logs/cron"
RUN_DATE="$(date '+%Y-%m-%d')"
RUN_TS="$(date '+%Y-%m-%d_%H-%M-%S')"

USER_IDS=("XJ1877" "OMK569" "SQW865" "DKJ644")
MONTHS=("near" "next")

mkdir -p "$LOG_DIR"

cd "$APP_DIR"

set -a
. "$APP_DIR/.env"
set +a

KITE_SERVICES_BASE_DIR="$APP_DIR" \
  source "$APP_DIR/scripts/runtime/configure_host_database_runtime.sh"

echo "==================================================" >> "$LOG_DIR/stop_cron_$RUN_DATE.log"
echo "STOP CRON STARTED: $RUN_TS" >> "$LOG_DIR/stop_cron_$RUN_DATE.log"
echo "==================================================" >> "$LOG_DIR/stop_cron_$RUN_DATE.log"

for USER_ID in "${USER_IDS[@]}"; do
  for MONTH in "${MONTHS[@]}"; do
    LOG_FILE="$LOG_DIR/stop_${USER_ID}_${MONTH}_${RUN_DATE}.log"

    {
      echo ""
      echo "--------------------------------------------------"
      echo "START: $(date '+%Y-%m-%d %H:%M:%S')"
      echo "USER_ID: $USER_ID"
      echo "MONTH: $MONTH"
      echo "COMMAND: stop_orchestrator.py $USER_ID $MONTH false"
      echo "--------------------------------------------------"

      PYTHONPATH=.:services "$APP_DIR/venv/bin/python" \
        engines/stop_engine/stop_orchestrator.py "$USER_ID" "$MONTH" false

      echo "--------------------------------------------------"
      echo "END: $(date '+%Y-%m-%d %H:%M:%S')"
      echo "STATUS: SUCCESS"
      echo "--------------------------------------------------"
    } >> "$LOG_FILE" 2>&1 || {
      {
        echo "--------------------------------------------------"
        echo "END: $(date '+%Y-%m-%d %H:%M:%S')"
        echo "STATUS: FAILED"
        echo "USER_ID: $USER_ID"
        echo "MONTH: $MONTH"
        echo "LOG_FILE: $LOG_FILE"
        echo "--------------------------------------------------"
      } >> "$LOG_DIR/stop_cron_$RUN_DATE.log"
      continue
    }

    echo "SUCCESS | $USER_ID | $MONTH | $LOG_FILE" >> "$LOG_DIR/stop_cron_$RUN_DATE.log"
  done
done

echo "STOP CRON FINISHED: $(date '+%Y-%m-%d_%H-%M-%S')" >> "$LOG_DIR/stop_cron_$RUN_DATE.log"
