# Stop Engine

This module contains the pure stop-computation engine, its dry-run adapter, tests, and module documentation.

## Current Files

```text
engines/stop_engine/
  __init__.py
  stop_computation_engine.py
  stop_computation_dry_run.py
  tests/
    test_stop_computation_engine.py
  docs/
    README.md
```

## Purpose

This module is intended to separate stop computation from persistence and execution.

Current scope:

- pure end-of-day stop computation
- real-data dry run using broker positions and completed daily candles
- unit tests for stop-computation rules

Not yet in scope:

- persistence of stop history
- persistence of active stop state
- lifecycle-derived origination integration
- live GTT placement / modification through the new architecture

## Architecture

Current layered structure:

1. `stop_computation_engine.py`
   - pure computation only
   - no broker API calls
   - no persistence
   - no order placement

2. `stop_computation_dry_run.py`
   - fetches real Kite positions
   - fetches completed daily candles
   - calls the pure computation engine
   - prints dry-run output only
   - does not place or modify any order

3. `tests/test_stop_computation_engine.py`
   - validates core stop rules

## EOD Compliance

The current computation engine is end-of-day aligned.

That means:

- stop computation uses completed daily candles only
- current price reference is the latest completed daily close
- no live bid / ask / last-traded price is used inside the pure computation engine
- live broker interaction is limited to data retrieval in the dry-run adapter

## Main Files

### `stop_computation_engine.py`

Core pure computation module.

Main functions:

- `compute_deterministic_stop_eod(...)`
- `prepare_limit_order_from_trigger(...)`

`compute_deterministic_stop_eod(...)` computes:

- ATR
- ATR average
- multiplier
- swing low / swing high
- initial stop
- trailing candidate
- raw stop
- validated stop
- trigger price
- update-required flag

It requires:

- completed candles
- side
- tick size
- entry price
- previous trigger price, if available

### `stop_computation_dry_run.py`

Real-data dry-run adapter.

It:

- reads real positions for a user
- selects near / next / far futures contracts
- fetches completed daily candles
- optionally reads existing GTT trigger price only for monotonicity / update-needed comparison
- computes stop output using the pure engine
- prints a dry-run table

It does not:

- place GTT orders
- modify GTT orders
- persist stop state
- persist stop history

## Run Tests

From project root (`kite_services`):

```bash
python -m unittest engines.stop_engine.tests.test_stop_computation_engine
```

## Run Real-Data Dry Run

From project root (`kite_services`):

```bash
PYTHONPATH=.:services python engines/stop_engine/stop_computation_dry_run.py OMK569
```

For a specific symbol:

```bash
PYTHONPATH=.:services python engines/stop_engine/stop_computation_dry_run.py OMK569 near HDFCBANK
```

General format:

```bash
PYTHONPATH=.:services python engines/stop_engine/stop_computation_dry_run.py <USER_ID> <contract_type> <SYMBOL>
```

Examples:

```bash
PYTHONPATH=.:services python engines/stop_engine/stop_computation_dry_run.py OMK569 near BPCL
PYTHONPATH=.:services python engines/stop_engine/stop_computation_dry_run.py OMK569 near INFY
PYTHONPATH=.:services python engines/stop_engine/stop_computation_dry_run.py OMK569 next HDFCBANK
```

## Validated So Far

The following have been validated in the current implementation flow:

- historical trade ingestion bootstrap for `OMK569`
- current open positions from database match KiteConnect positions
- expired contracts are excluded when deriving current open positions
- pure stop computation engine runs separately from broker execution
- real-data dry run works using broker positions and completed daily candles

## Current Limitations

The current stop-engine path still does not do the following:

- derive entry price or origination from persisted lifecycle state
- persist accepted stop states
- persist stop history
- answer authoritative historical questions such as “what was the actual stop yesterday?”
- execute live stop placement through the new engine architecture

## Next Planned Step

The next planned layer is persistence.

Recommended persistence tables:

- `stop_state_history`
- `active_stop_state`

That layer should store, at minimum:

- account_id
- instrument_key
- tradingsymbol
- side
- quantity
- entry_price
- calculation_date
- previous_trigger_price
- trigger_price
- limit_price
- ATR
- ATR average
- multiplier
- swing values
- raw stop
- validated stop
- update-required flag
- source mode
- broker trigger id, if available

## Summary

This module is now the computation-first foundation for the stop system.

Current status:

- computation engine exists
- dry-run adapter exists
- tests exist
- persistence is next
- live execution should come only after persistence is in place
