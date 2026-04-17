# Trade Ingestion Usage

## Purpose

This document explains how to use the data loaded by the historical trade ingestion process.

The historical ingestion layer writes broker-executed trade events into the `raw_broker_trades` table. This table is the append-only raw event store for imported broker trades. It is intended to serve as the source of truth for downstream lifecycle reconstruction, current position derivation, audit, and reconciliation workflows.

This document does not define stop-loss logic, execution logic, or risk management rules.

## What the Ingestion Layer Produces

The historical trade ingestion process populates two main tables:

- `raw_broker_trades`
- `trade_ingestion_runs`

### `raw_broker_trades`

This is the primary raw trade event store.

Each row represents one accepted broker trade event. Key stored fields include:

- `broker`
- `account_id`
- `source_type`
- `source_file_name`
- `source_row_number`
- `source_unique_key`
- `instrument_key`
- `symbol`
- `exchange`
- `segment`
- `expiry_date`
- `trade_id`
- `order_id`
- `execution_timestamp`
- `trade_date`
- `side`
- `signed_quantity`
- `quantity`
- `price`
- `auction`
- `isin`
- `series`
- `raw_payload`
- `ingestion_run_id`

### `trade_ingestion_runs`

This table stores import run metadata, including:

- run status
- files imported
- rows seen
- rows inserted
- rows ignored as duplicates
- error text if the run failed

## Accepted Uniqueness Rule

Historical trade uniqueness is enforced through the composite source key:

```text
broker|account_id|trade_id|order_id|order_execution_time
```

This value is stored in `source_unique_key`.

This key ensures that rerunning the same historical import does not create duplicate accepted trade rows.

## Source of Truth

The `raw_broker_trades` table should be treated as the source of truth for imported historical broker trade events.

It should be used as the input layer for:

- lifecycle reconstruction
- current net position computation
- trade origination derivation
- later reconciliation against refreshed broker tradebooks

Broker position snapshots should not replace this table for lifecycle derivation.

## How to Use the Data

## 1. Read Full Trade History for an Account

Use this when you want the complete imported trade history for one broker account.

```sql
SELECT *
FROM raw_broker_trades
WHERE broker = 'zerodha'
  AND account_id = 'OMK569'
ORDER BY execution_timestamp, trade_id;
```

## 2. Compute Current Net Quantity Per Instrument

Use `signed_quantity` to derive net position by instrument.

Rules already encoded in the imported data:

- `BUY` trades have positive `signed_quantity`
- `SELL` trades have negative `signed_quantity`

Query:

```sql
SELECT
    instrument_key,
    SUM(signed_quantity) AS net_quantity
FROM raw_broker_trades
WHERE broker = 'zerodha'
  AND account_id = 'OMK569'
GROUP BY instrument_key
ORDER BY instrument_key;
```

Interpretation:

- `net_quantity > 0` → long
- `net_quantity < 0` → short
- `net_quantity = 0` → flat

## 3. Find Currently Open Positions

Open positions are instruments whose cumulative signed quantity is not zero.

```sql
SELECT
    instrument_key,
    SUM(signed_quantity) AS net_quantity
FROM raw_broker_trades
WHERE broker = 'zerodha'
  AND account_id = 'OMK569'
GROUP BY instrument_key
HAVING SUM(signed_quantity) <> 0
ORDER BY instrument_key;
```
Use this query to get currently open positions from `raw_broker_trades`, excluding expired contracts:

```bash
docker exec -it local-postgres psql -U postgres -d trades -c "SELECT instrument_key, expiry_date, SUM(signed_quantity) AS net_quantity FROM raw_broker_trades WHERE broker = 'zerodha' AND account_id = 'OMK569' GROUP BY instrument_key, expiry_date HAVING SUM(signed_quantity) <> 0 AND (expiry_date IS NULL OR expiry_date >= CURRENT_DATE) ORDER BY instrument_key;"
```

This query:
- groups trades by `instrument_key` and `expiry_date`
- computes net quantity using `SUM(signed_quantity)`
- excludes zero net positions
- excludes expired contracts using `expiry_date >= CURRENT_DATE`

## 4. Audit Imported Trades

Use the raw fields for traceability and validation.

Useful audit columns:

- `source_file_name`
- `source_row_number`
- `trade_id`
- `order_id`
- `execution_timestamp`
- `raw_payload`

Example:

```sql
SELECT
    source_file_name,
    source_row_number,
    trade_id,
    order_id,
    execution_timestamp,
    raw_payload
FROM raw_broker_trades
WHERE broker = 'zerodha'
  AND account_id = 'OMK569'
ORDER BY execution_timestamp, trade_id
LIMIT 50;
```

## 5. Confirm Idempotent Import Behavior

To verify that the same historical file will not be inserted twice, re-run the importer and check that:

- `rows_inserted = 0`
- `rows_ignored_duplicate = full file row count`

You can also confirm directly in SQL that no accepted duplicate keys exist:

```sql
SELECT source_unique_key, COUNT(*)
FROM raw_broker_trades
GROUP BY source_unique_key
HAVING COUNT(*) > 1;
```

Expected result:

- `0 rows`

## 6. Reconciliation with Refreshed Historical Files

If a broker provides a refreshed or overlapping historical tradebook later:

1. import it again through the same importer
2. the importer will insert only unseen rows
3. already accepted rows will be ignored through `source_unique_key`
4. downstream lifecycle state can then be recomputed from `raw_broker_trades`

This means the ingestion layer supports safe historical backfill and refresh.

## What the Ingestion Layer Does Not Do

The ingestion layer does not itself compute:

- lifecycle boundaries
- trade origination timestamp
- current lifecycle identifier
- position state tables
- stop-loss state
- execution logic
- risk logic

Those should be implemented in downstream processing layers.

## Recommended Downstream Layers

The next logical layer should read from `raw_broker_trades` and produce derived state tables such as:

- `position_lifecycles`
- `current_position_state`
- `trade_origination_state`

That downstream process should:

1. read trades ordered by:
   - `instrument_key`
   - `execution_timestamp`
   - `trade_id`

2. compute cumulative signed quantity per instrument

3. detect lifecycle boundaries:
   - lifecycle starts when net quantity goes from zero to non-zero
   - lifecycle ends when net quantity returns to zero

4. assign lifecycle identifiers

5. define trade origination timestamp as the first trade timestamp in the active lifecycle

## Recommended Ordering for Reconstruction

When reconstructing lifecycle state from `raw_broker_trades`, use deterministic ordering:

1. `execution_timestamp`
2. `trade_id`

If needed for additional tie-breaking, include:

3. `order_id`

The ingestion table already stores all three fields.

## Practical Use Cases

The current ingestion data can be used immediately for:

- trade history audit
- raw trade inspection
- open position discovery
- per-instrument net quantity checks
- historical backfill reconciliation
- input to lifecycle reconstruction jobs

## Current Local Example

Validated local setup used during implementation:

- PostgreSQL host: `localhost`
- PostgreSQL port: `5432`
- database: `trades`
- user: `postgres`
- account imported: `OMK569`
- broker: `zerodha`

Imported historical row count:

- `11532`

## Example Validation Queries

### Count all imported raw trades

```sql
SELECT COUNT(*) AS raw_trade_count
FROM raw_broker_trades;
```

### Count imported rows by broker and account

```sql
SELECT broker, account_id, COUNT(*)
FROM raw_broker_trades
GROUP BY broker, account_id;
```

### Inspect first few accepted rows

```sql
SELECT
    source_unique_key,
    trade_id,
    order_id,
    execution_timestamp
FROM raw_broker_trades
ORDER BY id
LIMIT 5;
```

### Count open instruments

```sql
SELECT COUNT(*)
FROM (
    SELECT instrument_key
    FROM raw_broker_trades
    WHERE broker = 'zerodha'
      AND account_id = 'OMK569'
    GROUP BY instrument_key
    HAVING SUM(signed_quantity) <> 0
) t;
```

## Recommended Storage Location

Store this document at:

```text
trade_ingestion/docs/TRADE_INGESTION_USAGE.md
```

This keeps usage guidance separate from the setup/runbook material in:

```text
trade_ingestion/README_trade_ingestion.md
```

## Summary

The historical ingestion layer has one responsibility: store accepted broker trade events in a deterministic, append-only raw trade table.

Use `raw_broker_trades` as the raw event source for all downstream position and lifecycle logic.

Do not build lifecycle or net position logic directly from CSV files once they have been imported. Use the database table instead.
