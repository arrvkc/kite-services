# Strategy Engine Production Batch Files

This package contains the production-facing files for the Strategy Engine ingestion flow that consumes already computed Trend Identifier outputs for the NSE F&O stock universe.

## Included files

- `engines/strategy_engine/adapters/trend_identifier_adapter.py`
- `engines/strategy_engine/adapters/trend_identifier_batch_adapter.py`
- `engines/strategy_engine/scripts/build_trend_history_csv_for_fo_universe.py`
- `engines/strategy_engine/scripts/build_contract_snapshot_csv_for_fo_universe.py`
- `engines/strategy_engine/scripts/run_strategy_engine_adapter_for_symbol.py`
- `engines/strategy_engine/scripts/run_strategy_engine_batch_from_csv.py`

## Dependency expectations

These files are designed to sit inside your existing repository and rely on the modules you already have:

- `services.kite_credentials_service`
- `engines.trend_identifier.trend_identifier.runners.equity_trend_runner`
- `engines.trend_identifier.trend_identifier.runners.equity_trend_history_runner`
- `engines.strategy_engine.models`
- `engines.strategy_engine.engine`

## Recommended workflow

### 1. Quick one-symbol adapter check

```bash
PYTHONPATH=.:services python engines/strategy_engine/scripts/run_strategy_engine_adapter_for_symbol.py OMK569 ABB
```

This prints a compact fixed-width summary showing:

- upstream Trend Identifier label
- confidence
- aggregate score
- W5 directional persistence counts
- near-month DTE
- next-month DTE
- a likely family preview based on the strong directional rules

### 2. Build combined Trend Identifier history CSV for the full F&O stock universe

```bash
PYTHONPATH=.:services python engines/strategy_engine/scripts/build_trend_history_csv_for_fo_universe.py OMK569 --history-days 5 --output data/trend_history_fo_universe.csv
```

This step is intentionally separate because it is the expensive precompute step. It calls the existing Trend Identifier history runner once per symbol and writes one combined CSV.

Expected output files:

- `data/trend_history_fo_universe.csv`
- `data/trend_history_fo_universe_failures.csv` when some symbols fail

Expected columns in the main CSV:

- `symbol`
- `date`
- `close`
- `label`
- `confidence`
- `aggregate_score`
- `internal_state`
- `exchange`
- `tradingsymbol`
- `instrument_token`

### 3. Build contract snapshot CSV for the full F&O stock universe

```bash
PYTHONPATH=.:services python engines/strategy_engine/scripts/build_contract_snapshot_csv_for_fo_universe.py OMK569 --output data/contract_snapshot_fo_universe.csv
```

Expected output files:

- `data/contract_snapshot_fo_universe.csv`
- `data/contract_snapshot_fo_universe_failures.csv` when some symbols fail

Expected columns in the main CSV:

- `symbol`
- `selection_date`
- `near_expiry`
- `next_expiry`
- `dte_near_month`
- `next_month_available`
- `dte_next_month`

### 4. Run Strategy Engine batch from the precomputed CSVs

```bash
PYTHONPATH=.:services python engines/strategy_engine/scripts/run_strategy_engine_batch_from_csv.py \
  --trend-history-csv data/trend_history_fo_universe.csv \
  --contract-snapshot-csv data/contract_snapshot_fo_universe.csv
```

This script:

- loads the two CSVs
- adapts them into `StrategyInput` objects
- runs `evaluate_batch(...)`
- prints ranked Strategy Engine output in a fixed-width table

The printed output includes both final decisions and the key decision-driving inputs:

- `LABEL`
- `SCORE`
- `CONF`
- `STATE`
- `BULL5`
- `BEAR5`
- `FLAT5`
- `SIGNFLIP5`
- `MEAN3`
- `NEAR_DTE`
- `NEXT_DTE`
- `CANDIDATE_FAMILY`
- `STRATEGY_FAMILY`
- `CONTRACT_MONTH`
- `STRENGTH`
- `TOP_N`
- `RANK_ALL`
- `RANK_FAMILY`
- `TRANSITION_STATE`
- `REASONS`

### 5. Write the batch output to CSV

```bash
PYTHONPATH=.:services python engines/strategy_engine/scripts/run_strategy_engine_batch_from_csv.py \
  --trend-history-csv data/trend_history_fo_universe.csv \
  --contract-snapshot-csv data/contract_snapshot_fo_universe.csv \
  --output-csv data/strategy_engine_batch_output.csv
```

This writes the same columns shown in the terminal table into a CSV file while still printing the table to stdout.

## Why this flow is faster than the direct full-universe adapter path

The direct live-universe adapter path reruns Trend Identifier multiple times per symbol. For a large F&O universe, that becomes very expensive.

This package separates the expensive Trend Identifier history generation from the Strategy Engine run:

1. compute and save Trend history once
2. compute and save contract snapshot once
3. run Strategy Engine off those saved files

That avoids repeated recomputation when you rerun the Strategy Engine.

## Important implementation truth

The Trend Identifier side in these files still uses:

- `EquityTrendRunner`
- `EquityTrendHistoryRunner`

That means the upstream trend is being read from the equity symbol series, while contract availability and DTE come from the NFO universe. This matches the logic used in the files discussed in the chat and should be treated as intentional unless you later decide to change the upstream trend source.

## Install / placement

Copy these files into the matching locations inside your repository. They are not standalone by themselves; they are designed to plug into your existing codebase.
