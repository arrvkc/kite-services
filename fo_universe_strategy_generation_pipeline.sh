#!/bin/bash

set -e

USER_ID="OMK569"
DATE=$(date +%Y%m%d)

echo "Step 1: Building trend history CSV..."
PYTHONPATH=.:services python engines/strategy_engine/scripts/build_trend_history_csv_for_fo_universe.py \
  $USER_ID \
  --history-days 5 \
  --output data/trend_history_fo_universe.csv

echo "Step 2: Building contract snapshot CSV..."
PYTHONPATH=.:services python engines/strategy_engine/scripts/build_contract_snapshot_csv_for_fo_universe.py \
  $USER_ID \
  --output data/contract_snapshot_fo_universe.csv

echo "Step 3: Running strategy engine batch..."
PYTHONPATH=.:services python engines/strategy_engine/scripts/run_strategy_engine_batch_from_csv.py \
  --trend-history-csv data/trend_history_fo_universe.csv \
  --contract-snapshot-csv data/contract_snapshot_fo_universe.csv \
  --output-csv "data/strategy_engine_batch_output_${DATE}.csv"

echo "Pipeline completed successfully"
echo "Output file: data/strategy_engine_batch_output_${DATE}.csv"
