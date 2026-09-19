#!/bin/bash
set -euo pipefail

APP_DIR="${EAJEE_KITE_SERVICES_DIR:-/opt/kite_services}"
LOG_DIR="$APP_DIR/logs/strategy_db_pipeline"
PYTHON_BIN="${EAJEE_KITE_PYTHON:-$APP_DIR/venv/bin/python}"

RUN_DATE="${EAJEE_STRATEGY_RUN_DATE:-$(date '+%Y-%m-%d')}"
RUN_TS="$(date '+%Y-%m-%d_%H-%M-%S')"

USER_ID="XJ1877"
FINALITY_RETRY_ATTEMPTS="${EAJEE_STRATEGY_FINALITY_RETRY_ATTEMPTS:-3}"
FINALITY_RETRY_SECONDS="${EAJEE_STRATEGY_FINALITY_RETRY_SECONDS:-300}"

if [[ ! "$FINALITY_RETRY_ATTEMPTS" =~ ^[1-5]$ ]]; then
  echo "Invalid EAJEE_STRATEGY_FINALITY_RETRY_ATTEMPTS" >&2
  exit 64
fi
if [[ ! "$FINALITY_RETRY_SECONDS" =~ ^[0-9]+$ ]] || (( FINALITY_RETRY_SECONDS > 900 )); then
  echo "Invalid EAJEE_STRATEGY_FINALITY_RETRY_SECONDS" >&2
  exit 64
fi

mkdir -p "$LOG_DIR"

cd "$APP_DIR"

set -a
. "$APP_DIR/.env"
set +a

KITE_SERVICES_BASE_DIR="$APP_DIR" \
  source "$APP_DIR/scripts/runtime/configure_host_database_runtime.sh"

MASTER_LOG="$LOG_DIR/pipeline_$RUN_DATE.log"

echo "==================================================" >> "$MASTER_LOG"
echo "STRATEGY DB PIPELINE STARTED: $RUN_TS" >> "$MASTER_LOG"
echo "==================================================" >> "$MASTER_LOG"

{
  echo ""
  echo "--------------------------------------------------"
  echo "STEP 0: KITE CREDENTIAL PREFLIGHT"
  echo "START: $(date '+%Y-%m-%d %H:%M:%S')"
  echo "--------------------------------------------------"

  PYTHONPATH=.:services "$PYTHON_BIN" \
    engines/strategy_deterministic_engine/scripts/validate_strategy_kite_credentials.py \
    "$USER_ID"

  echo "STEP 0 COMPLETE"
} >> "$LOG_DIR/credential_preflight_$RUN_TS.log" 2>&1

{
  echo ""
  echo "--------------------------------------------------"
  echo "STEP 1: EXACT-DATE TREND HISTORY SYNC AND VALIDATION"
  echo "START: $(date '+%Y-%m-%d %H:%M:%S')"
  echo "--------------------------------------------------"

  attempt=1
  while true; do
    set +e
    PYTHONPATH=.:services "$PYTHON_BIN" \
      engines/strategy_deterministic_engine/scripts/sync_trend_history_fo_universe_to_db.py \
      "$USER_ID" \
      --history-days 5 \
      --end-date "$RUN_DATE" \
      --strict
    trend_rc=$?
    set -e
    if (( trend_rc == 0 )); then
      break
    fi
    if (( trend_rc != 75 || attempt >= FINALITY_RETRY_ATTEMPTS )); then
      exit "$trend_rc"
    fi
    echo "FINALITY_RETRY pending attempt=$attempt/$FINALITY_RETRY_ATTEMPTS"
    sleep "$FINALITY_RETRY_SECONDS"
    attempt=$((attempt + 1))
  done

  echo "--------------------------------------------------"
  echo "STEP 1 COMPLETE"
  echo "END: $(date '+%Y-%m-%d %H:%M:%S')"
  echo "--------------------------------------------------"

} >> "$LOG_DIR/trend_history_sync_$RUN_TS.log" 2>&1

{
  echo ""
  echo "--------------------------------------------------"
  echo "STEP 2: EXACT-DATE CONTRACT SNAPSHOT SYNC AND VALIDATION"
  echo "START: $(date '+%Y-%m-%d %H:%M:%S')"
  echo "--------------------------------------------------"

  PYTHONPATH=.:services "$PYTHON_BIN" \
    engines/strategy_deterministic_engine/scripts/sync_contract_snapshot_fo_universe_to_db.py \
    "$USER_ID" \
    --selection-date "$RUN_DATE" \
    --strict

  echo "--------------------------------------------------"
  echo "STEP 2 COMPLETE"
  echo "END: $(date '+%Y-%m-%d %H:%M:%S')"
  echo "--------------------------------------------------"

} >> "$LOG_DIR/contract_snapshot_sync_$RUN_TS.log" 2>&1

{
  echo ""
  echo "--------------------------------------------------"
  echo "STEP 3: EXACT-DATE INPUT GATE"
  echo "START: $(date '+%Y-%m-%d %H:%M:%S')"
  echo "--------------------------------------------------"

  PYTHONPATH=.:services "$PYTHON_BIN" \
    engines/strategy_deterministic_engine/scripts/verify_strategy_backfill_inputs.py \
    --run-date "$RUN_DATE" \
    --user-id "$USER_ID" \
    --history-days 5

  echo "--------------------------------------------------"
  echo "STEP 3 COMPLETE"
  echo "END: $(date '+%Y-%m-%d %H:%M:%S')"
  echo "--------------------------------------------------"

} >> "$LOG_DIR/exact_input_gate_$RUN_TS.log" 2>&1

{
  echo ""
  echo "--------------------------------------------------"
  echo "STEP 4: STRATEGY ENGINE RUN"
  echo "START: $(date '+%Y-%m-%d %H:%M:%S')"
  echo "--------------------------------------------------"

  PYTHONPATH=.:services "$PYTHON_BIN" \
    engines/strategy_deterministic_engine/scripts/run_strategy_engine_batch_from_db.py \
    "$USER_ID" \
    --run-date "$RUN_DATE" \
    --history-days 5 \
    --require-exact-contract-snapshot

  echo "--------------------------------------------------"
  echo "STEP 4 COMPLETE"
  echo "END: $(date '+%Y-%m-%d %H:%M:%S')"
  echo "--------------------------------------------------"

} >> "$LOG_DIR/strategy_engine_run_$RUN_TS.log" 2>&1

echo "==================================================" >> "$MASTER_LOG"
echo "STRATEGY DB PIPELINE FINISHED: $(date '+%Y-%m-%d_%H-%M-%S')" >> "$MASTER_LOG"
echo "==================================================" >> "$MASTER_LOG"
