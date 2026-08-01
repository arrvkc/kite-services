#!/bin/bash
set -euo pipefail

APP_DIR="/opt/kite_services"
LOG_DIR="$APP_DIR/logs/strategy_db_pipeline"

RUN_DATE="$(date '+%Y-%m-%d')"
RUN_TS="$(date '+%Y-%m-%d_%H-%M-%S')"

USER_ID="OMK569"

mkdir -p "$LOG_DIR"

cd "$APP_DIR"

set -a
. "$APP_DIR/.env"
set +a

MASTER_LOG="$LOG_DIR/pipeline_$RUN_DATE.log"

echo "==================================================" >> "$MASTER_LOG"
echo "STRATEGY DB PIPELINE STARTED: $RUN_TS" >> "$MASTER_LOG"
echo "==================================================" >> "$MASTER_LOG"

{
  echo ""
  echo "--------------------------------------------------"
  echo "STEP 1: TREND HISTORY SYNC"
  echo "START: $(date '+%Y-%m-%d %H:%M:%S')"
  echo "--------------------------------------------------"

  PYTHONPATH=.:services "$APP_DIR/venv/bin/python" \
    engines/strategy_deterministic_engine/scripts/sync_trend_history_fo_universe_to_db.py \
    "$USER_ID" \
    --history-days 180

  echo "--------------------------------------------------"
  echo "STEP 1 COMPLETE"
  echo "END: $(date '+%Y-%m-%d %H:%M:%S')"
  echo "--------------------------------------------------"

} >> "$LOG_DIR/trend_history_sync_$RUN_TS.log" 2>&1

{
  echo ""
  echo "--------------------------------------------------"
  echo "STEP 2: CONTRACT SNAPSHOT SYNC"
  echo "START: $(date '+%Y-%m-%d %H:%M:%S')"
  echo "--------------------------------------------------"

  PYTHONPATH=.:services "$APP_DIR/venv/bin/python" \
    engines/strategy_deterministic_engine/scripts/sync_contract_snapshot_fo_universe_to_db.py \
    "$USER_ID"

  echo "--------------------------------------------------"
  echo "STEP 2 COMPLETE"
  echo "END: $(date '+%Y-%m-%d %H:%M:%S')"
  echo "--------------------------------------------------"

} >> "$LOG_DIR/contract_snapshot_sync_$RUN_TS.log" 2>&1

{
  echo ""
  echo "--------------------------------------------------"
  echo "STEP 3: STRATEGY ENGINE RUN"
  echo "START: $(date '+%Y-%m-%d %H:%M:%S')"
  echo "--------------------------------------------------"

  PYTHONPATH=.:services "$APP_DIR/venv/bin/python" \
    engines/strategy_deterministic_engine/scripts/run_strategy_engine_batch_from_db.py \
    "$USER_ID"

  echo "--------------------------------------------------"
  echo "STEP 3 COMPLETE"
  echo "END: $(date '+%Y-%m-%d %H:%M:%S')"
  echo "--------------------------------------------------"

} >> "$LOG_DIR/strategy_engine_run_$RUN_TS.log" 2>&1

echo "==================================================" >> "$MASTER_LOG"
echo "STRATEGY DB PIPELINE FINISHED: $(date '+%Y-%m-%d_%H-%M-%S')" >> "$MASTER_LOG"
echo "==================================================" >> "$MASTER_LOG"
