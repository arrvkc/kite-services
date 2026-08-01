#!/bin/bash
set -euo pipefail

KITE_APP_DIR="/opt/kite_services"
LOG_DIR="/opt/kite_services/logs/fetch52w_atms"
EMAIL_SCRIPT="/opt/kite_services/send_fetch52w_atms_email.py"
ATMS_CONTAINER="atms"
CMD_PATH="./cli/commands/cmd_atms.py"

RUN_DATE="$(date '+%Y-%m-%d')"
RUN_TS="$(date '+%Y-%m-%d_%H-%M-%S')"
START_EPOCH="$(date +%s)"
STARTED_AT="$(date '+%Y-%m-%d %H:%M:%S')"

mkdir -p "$LOG_DIR"

cd "$KITE_APP_DIR"

set -a
. "$KITE_APP_DIR/.env"
set +a

MASTER_LOG="$LOG_DIR/fetch52w_$RUN_DATE.log"
RUN_LOG="$LOG_DIR/fetch52w_run_$RUN_TS.log"
EMAIL_LOG="$LOG_DIR/fetch52w_email_$RUN_TS.log"
CONTAINER_LOG="$LOG_DIR/fetch52w_container_$RUN_TS.log"

STATUS="SUCCESS"
EXIT_CODE=0

echo "==================================================" >> "$MASTER_LOG"
echo "FETCH52W ATMS CRON STARTED: $RUN_TS" >> "$MASTER_LOG"
echo "CONTAINER: $ATMS_CONTAINER" >> "$MASTER_LOG"
echo "CMD_PATH: $CMD_PATH" >> "$MASTER_LOG"
echo "RUN_LOG: $RUN_LOG" >> "$MASTER_LOG"
echo "CONTAINER_LOG: $CONTAINER_LOG" >> "$MASTER_LOG"
echo "==================================================" >> "$MASTER_LOG"

{
  echo ""
  echo "--------------------------------------------------"
  echo "FETCH52W COMMAND START"
  echo "START: $(date '+%Y-%m-%d %H:%M:%S')"
  echo "CONTAINER: $ATMS_CONTAINER"
  echo "CMD_PATH: $CMD_PATH"
  echo "--------------------------------------------------"

  set +e
  docker exec "$ATMS_CONTAINER" sh -lc "mkdir -p ./logs/fetch_52w && ATMS_FETCH52W_LOG_DIR=./logs/fetch_52w python $CMD_PATH fetch-52w-cmd --file-for BOTH" > "$CONTAINER_LOG" 2>&1
  EXIT_CODE=$?
  set -e

  if [ "$EXIT_CODE" -ne 0 ]; then
    STATUS="FAILED"
    echo "FETCH52W COMMAND FAILED"
  else
    echo "FETCH52W COMMAND COMPLETE"
  fi

  echo "END: $(date '+%Y-%m-%d %H:%M:%S')"
  echo "STATUS: $STATUS"
  echo "EXIT_CODE: $EXIT_CODE"
  echo ""
  echo "CONTAINER OUTPUT:"
  cat "$CONTAINER_LOG" || true
  echo "--------------------------------------------------"

} >> "$RUN_LOG" 2>&1

ENDED_AT="$(date '+%Y-%m-%d %H:%M:%S')"
END_EPOCH="$(date +%s)"
DURATION_SECONDS="$((END_EPOCH - START_EPOCH))"

ATMS_CLI_LOG_IN_CONTAINER="./logs/fetch_52w/fetch_52w_$RUN_DATE.log"
ATMS_CLI_LOG_ON_HOST="$LOG_DIR/atms_cli_fetch_52w_$RUN_TS.log"

set +e
docker exec "$ATMS_CONTAINER" sh -lc "cat $ATMS_CLI_LOG_IN_CONTAINER 2>/dev/null || true" > "$ATMS_CLI_LOG_ON_HOST" 2>&1
set -e

echo "FETCH52W ATMS CRON FINISHED: $(date '+%Y-%m-%d_%H-%M-%S')" >> "$MASTER_LOG"
echo "STATUS: $STATUS" >> "$MASTER_LOG"
echo "EXIT_CODE: $EXIT_CODE" >> "$MASTER_LOG"
echo "DURATION_SECONDS: $DURATION_SECONDS" >> "$MASTER_LOG"
echo "RUN_LOG: $RUN_LOG" >> "$MASTER_LOG"
echo "EMAIL_LOG: $EMAIL_LOG" >> "$MASTER_LOG"
echo "CONTAINER_LOG: $CONTAINER_LOG" >> "$MASTER_LOG"
echo "ATMS_CLI_LOG_ON_HOST: $ATMS_CLI_LOG_ON_HOST" >> "$MASTER_LOG"
echo "==================================================" >> "$MASTER_LOG"

set +e
"$KITE_APP_DIR/venv/bin/python" "$EMAIL_SCRIPT" \
  --status "$STATUS" \
  --exit-code "$EXIT_CODE" \
  --started-at "$STARTED_AT" \
  --ended-at "$ENDED_AT" \
  --duration-seconds "$DURATION_SECONDS" \
  --run-log "$RUN_LOG" \
  --master-log "$MASTER_LOG" \
  --atms-cli-log "$ATMS_CLI_LOG_ON_HOST" \
  --container-log "$CONTAINER_LOG" \
  --container-name "$ATMS_CONTAINER" \
  --cmd-path "$CMD_PATH" \
  --env-file "$KITE_APP_DIR/.env" \
  >> "$EMAIL_LOG" 2>&1

EMAIL_EXIT=$?
set -e

echo "EMAIL_EXIT_CODE: $EMAIL_EXIT" >> "$MASTER_LOG"

if [ "$EMAIL_EXIT" -ne 0 ]; then
  echo "EMAIL_STATUS: FAILED" >> "$MASTER_LOG"
else
  echo "EMAIL_STATUS: SENT" >> "$MASTER_LOG"
fi

exit "$EXIT_CODE"
