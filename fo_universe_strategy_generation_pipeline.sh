#!/bin/bash

set -e

USER_ID="OMK569"
DATE=$(date +%Y%m%d)

TREND_CSV="data/trend_history_fo_universe_${DATE}.csv"
CONTRACT_CSV="data/contract_snapshot_fo_universe_${DATE}.csv"
OUTPUT_CSV="data/strategy_deterministic_engine_batch_output_${DATE}.csv"

echo "Step 2: Building Trend Identifier history CSV..."
PYTHONPATH=.:services python engines/strategy_deterministic_engine/scripts/build_trend_history_csv_for_fo_universe.py \
  "$USER_ID" \
  --history-days 5 \
  --output "$TREND_CSV"

echo "Step 3: Building contract snapshot CSV..."
PYTHONPATH=.:services python engines/strategy_deterministic_engine/scripts/build_contract_snapshot_csv_for_fo_universe.py \
  "$USER_ID" \
  --output "$CONTRACT_CSV"

echo "Step 5: Running Strategy Deterministic Engine batch..."
PYTHONPATH=.:services python engines/strategy_deterministic_engine/scripts/run_strategy_engine_batch_from_csv.py \
  --trend-history-csv "$TREND_CSV" \
  --contract-snapshot-csv "$CONTRACT_CSV" \
  --output-csv "$OUTPUT_CSV"

echo "Pipeline completed successfully"
echo "Trend CSV: $TREND_CSV"
echo "Contract CSV: $CONTRACT_CSV"
echo "Output CSV: $OUTPUT_CSV"
