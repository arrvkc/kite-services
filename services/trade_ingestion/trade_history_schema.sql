CREATE TABLE IF NOT EXISTS trade_ingestion_runs (
    id BIGSERIAL PRIMARY KEY,
    broker TEXT NOT NULL,
    account_id TEXT NOT NULL,
    run_label TEXT NULL,
    run_type TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ NULL,
    status TEXT NOT NULL DEFAULT 'RUNNING',
    files_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    rows_seen BIGINT NOT NULL DEFAULT 0,
    rows_inserted BIGINT NOT NULL DEFAULT 0,
    rows_ignored_duplicate BIGINT NOT NULL DEFAULT 0,
    error_text TEXT NULL
);

CREATE TABLE IF NOT EXISTS raw_broker_trades (
    id BIGSERIAL PRIMARY KEY,
    broker TEXT NOT NULL,
    account_id TEXT NOT NULL,
    source_type TEXT NOT NULL,
    source_file_name TEXT NOT NULL,
    source_row_number INTEGER NOT NULL,
    source_unique_key TEXT NOT NULL,
    instrument_key TEXT NOT NULL,
    symbol TEXT NOT NULL,
    exchange TEXT NULL,
    segment TEXT NULL,
    expiry_date DATE NULL,
    trade_id TEXT NOT NULL,
    order_id TEXT NULL,
    execution_timestamp TIMESTAMP NOT NULL,
    trade_date DATE NULL,
    side TEXT NOT NULL,
    signed_quantity NUMERIC(20,6) NOT NULL,
    quantity NUMERIC(20,6) NOT NULL,
    price NUMERIC(20,6) NOT NULL,
    auction BOOLEAN NULL,
    isin TEXT NULL,
    series TEXT NULL,
    raw_payload JSONB NOT NULL,
    imported_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ingestion_run_id BIGINT NULL REFERENCES trade_ingestion_runs(id)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_raw_broker_trades_source_key
    ON raw_broker_trades(source_unique_key);

CREATE INDEX IF NOT EXISTS ix_raw_broker_trades_instrument_ts
    ON raw_broker_trades(instrument_key, execution_timestamp, trade_id);

CREATE INDEX IF NOT EXISTS ix_raw_broker_trades_account_ts
    ON raw_broker_trades(account_id, execution_timestamp);
