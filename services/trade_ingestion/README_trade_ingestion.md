# Trade Ingestion

This module contains the scripts and artifacts used for historical tradebook ingestion for Zerodha account `OMK569`.

## Folder Structure

```text
trade_ingestion/
  __init__.py
  import_historical_tradebooks.py
  merge_historical_tradebooks.py
  find_tradebook_duplicates.py
  trade_history_schema.sql
  README_trade_ingestion.md
  data/
    raw/
      tradebook-OMK569-FO 24-25.csv
      tradebook-OMK569-FO 25-25.csv
      tradebook-OMK569-FO 26-27.csv
    merged/
      omk569_fo_historical_bootstrap.csv
    reports/
      omk569_duplicate_report.csv
      omk569_duplicate_report_v2.csv
```

## Database

Local PostgreSQL values used:

- Host: `localhost`
- Port: `5432`
- Database: `trades`
- User: `postgres`
- Password: `postgres`

DSN:

```bash
postgresql://postgres:postgres@localhost:5432/trades
```

Docker container:

```bash
local-postgres
```

## Historical Bootstrap Flow

### 1. Start PostgreSQL

```bash
docker compose up -d
```

### 2. Merge historical raw broker tradebooks

```bash
python trade_ingestion/merge_historical_tradebooks.py \
  --broker zerodha \
  --account-id OMK569 \
  --output trade_ingestion/data/merged/omk569_fo_historical_bootstrap.csv \
  "trade_ingestion/data/raw/tradebook-OMK569-FO 24-25.csv" \
  "trade_ingestion/data/raw/tradebook-OMK569-FO 25-25.csv" \
  "trade_ingestion/data/raw/tradebook-OMK569-FO 26-27.csv"
```

Validated result after fixing timestamp parsing:

- `rows_seen=11532`
- `rows_written=11532`
- `rows_ignored_duplicate=0`

Note:
An earlier weak-key version of the merge logic incorrectly treated 40 valid rows as duplicates because it used only `broker|account_id|trade_id`. That logic was replaced with the stronger composite key documented below.

### 3. Check duplicates with the stronger composite key

```bash
python trade_ingestion/find_tradebook_duplicates.py \
  --broker zerodha \
  --account-id OMK569 \
  --output trade_ingestion/data/reports/omk569_duplicate_report_v2.csv \
  "trade_ingestion/data/raw/tradebook-OMK569-FO 24-25.csv" \
  "trade_ingestion/data/raw/tradebook-OMK569-FO 25-25.csv" \
  "trade_ingestion/data/raw/tradebook-OMK569-FO 26-27.csv"
```

Validated result:

- `duplicate_groups=0`
- `duplicate_rows=0`

## Duplicate Definition

The accepted raw trade uniqueness rule is:

```text
broker|account_id|trade_id|order_id|order_execution_time
```

Example stored key:

```text
zerodha|OMK569|980348129|2700000031311054|2024-08-09T10:01:06
```

This corrected an earlier weak key that used only:

```text
broker|account_id|trade_id
```

## Import Historical Bootstrap into PostgreSQL

```bash
python trade_ingestion/import_historical_tradebooks.py \
  --dsn "postgresql://postgres:postgres@localhost:5432/trades" \
  --broker zerodha \
  --account-id OMK569 \
  --create-schema \
  "trade_ingestion/data/merged/omk569_fo_historical_bootstrap.csv"
```

Validated result:

- `rows_seen=11532`
- `rows_inserted=11532`
- `rows_ignored_duplicate=0`

## Validation Queries

### Total imported raw trades

```bash
docker exec -it local-postgres psql -U postgres -d trades -c "SELECT COUNT(*) AS raw_trade_count FROM raw_broker_trades;"
```

Validated result:

- `11532`

### Check duplicate accepted keys

```bash
docker exec -it local-postgres psql -U postgres -d trades -c "SELECT source_unique_key, COUNT(*) FROM raw_broker_trades GROUP BY source_unique_key HAVING COUNT(*) > 1 LIMIT 20;"
```

Validated result:

- `0 rows`

### Inspect stored composite keys

```bash
docker exec -it local-postgres psql -U postgres -d trades -c "SELECT source_unique_key, trade_id, order_id, execution_timestamp FROM raw_broker_trades ORDER BY id LIMIT 5;"
```

Sample validated output shape:

```text
zerodha|OMK569|980348129|2700000031311054|2024-08-09T10:01:06
zerodha|OMK569|980348130|2700000031311054|2024-08-09T10:01:06
zerodha|OMK569|980348131|2700000031311054|2024-08-09T10:01:06
```

### Check imported account scope

```bash
docker exec -it local-postgres psql -U postgres -d trades -c "SELECT broker, account_id, COUNT(*) FROM raw_broker_trades GROUP BY broker, account_id;"
```

Validated result:

- `zerodha | OMK569 | 11532`

## Idempotency Test

Re-run the same import:

```bash
python trade_ingestion/import_historical_tradebooks.py \
  --dsn "postgresql://postgres:postgres@localhost:5432/trades" \
  --broker zerodha \
  --account-id OMK569 \
  --create-schema \
  "trade_ingestion/data/merged/omk569_fo_historical_bootstrap.csv"
```

Expected:

- `rows_seen=11532`
- `rows_inserted=0`
- `rows_ignored_duplicate=11532`

This proves the import is rerunnable and safe.

## Apply for Another User

Only change:

- `--account-id`
- raw input file names
- merged output file name
- duplicate report name

Example shape:

```bash
python trade_ingestion/merge_historical_tradebooks.py \
  --broker zerodha \
  --account-id XJ1877 \
  --output trade_ingestion/data/merged/xj1877_historical_bootstrap.csv \
  "trade_ingestion/data/raw/tradebook-XJ1877-FO-1.csv"
```

Then import with the same DSN:

```bash
python trade_ingestion/import_historical_tradebooks.py \
  --dsn "postgresql://postgres:postgres@localhost:5432/trades" \
  --broker zerodha \
  --account-id XJ1877 \
  --create-schema \
  "trade_ingestion/data/merged/xj1877_historical_bootstrap.csv"
```

## Notes

- Do not truncate tables in normal operation.
- `raw_broker_trades` is append-only.
- `trade_ingestion_runs` stores audit history for each import.
- Historical tradebook import is the raw-ingestion layer only.
- Lifecycle reconstruction, current position state, and origination timestamp derivation should be implemented as separate downstream steps.

## Recommended Next Step

Build a lifecycle reconstruction job that reads `raw_broker_trades`, orders trades by `execution_timestamp`, computes cumulative net position, derives lifecycle boundaries, and writes:

- lifecycle state
- current position state
- trade origination timestamp
