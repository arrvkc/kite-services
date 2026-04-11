#!/usr/bin/env python3
"""Import broker-exported historical tradebooks into PostgreSQL.

Purpose:
- Load one or more broker tradebook CSV files into an append-only raw trade table.
- Normalize data into a canonical schema.
- Suppress duplicates using a deterministic source key.
- Support rerunnable sync behavior for historical backoffice files.

Assumptions based on uploaded tradebooks:
- CSV columns include:
  symbol, isin, trade_date, exchange, segment, series, trade_type,
  auction, quantity, price, trade_id, order_id, order_execution_time,
  expiry_date
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sys
from dataclasses import dataclass
from datetime import datetime, date
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Iterable, Iterator, Optional

import psycopg
from psycopg.rows import dict_row


REQUIRED_COLUMNS = {
    "symbol",
    "trade_type",
    "quantity",
    "price",
    "trade_id",
    "order_execution_time",
}


@dataclass(frozen=True)
class CanonicalTrade:
    broker: str
    account_id: str
    source_file_name: str
    source_row_number: int
    instrument_key: str
    symbol: str
    exchange: Optional[str]
    segment: Optional[str]
    expiry_date: Optional[date]
    trade_id: str
    order_id: Optional[str]
    execution_timestamp: datetime
    trade_date: Optional[date]
    side: str
    signed_quantity: Decimal
    quantity: Decimal
    price: Decimal
    auction: Optional[bool]
    isin: Optional[str]
    series: Optional[str]
    raw_payload: dict
    source_unique_key: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Import historical broker tradebook CSV files into PostgreSQL"
    )
    parser.add_argument(
        "files",
        nargs="+",
        help="One or more tradebook CSV paths",
    )
    parser.add_argument(
        "--dsn",
        required=True,
        help="PostgreSQL DSN, e.g. postgresql://user:pass@host:5432/dbname",
    )
    parser.add_argument(
        "--broker",
        default="zerodha",
        help="Broker source name",
    )
    parser.add_argument(
        "--account-id",
        required=True,
        help="Broker account / Zerodha user ID",
    )
    parser.add_argument(
        "--create-schema",
        action="store_true",
        help="Create required tables before import",
    )
    parser.add_argument(
        "--run-label",
        default=None,
        help="Optional label for this sync run",
    )
    return parser.parse_args()


def normalize_side(raw_side: str) -> str:
    value = (raw_side or "").strip().upper()
    if value == "BUY":
        return "BUY"
    if value == "SELL":
        return "SELL"
    raise ValueError(f"Unsupported trade_type value: {raw_side!r}")


def parse_decimal(value: object, field_name: str) -> Decimal:
    try:
        return Decimal(str(value).strip())
    except (InvalidOperation, AttributeError) as exc:
        raise ValueError(f"Invalid decimal for {field_name}: {value!r}") from exc


def parse_optional_date(value: object) -> Optional[date]:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return None
    return date.fromisoformat(text)


def parse_required_datetime(value: object) -> datetime:
    text = str(value).strip()
    if not text or text.lower() == "nan":
        raise ValueError("Missing order_execution_time")
    return datetime.fromisoformat(text)


def normalize_text(value: object) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return None
    return text


def build_instrument_key(row: dict) -> str:
    # Futures/options should not be keyed only by bare symbol root.
    # Use the broker-reported contract symbol plus exchange/segment where available.
    symbol = normalize_text(row.get("symbol"))
    exchange = normalize_text(row.get("exchange")) or ""
    segment = normalize_text(row.get("segment")) or ""
    if not symbol:
        raise ValueError("Missing symbol")
    return f"{exchange}|{segment}|{symbol}"


def build_source_unique_key(
    broker: str,
    account_id: str,
    trade_id: str,
    order_id: str,
    order_execution_time: str,
) -> str:
    return "|".join(
        [
            broker.strip(),
            account_id.strip(),
            trade_id.strip(),
            order_id.strip(),
            order_execution_time.strip(),
        ]
    )


SCHEMA_SQL = """
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
"""


def ensure_schema(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(SCHEMA_SQL)
    conn.commit()


def iter_csv_rows(path: Path) -> Iterator[tuple[int, dict]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise ValueError(f"CSV file has no header: {path}")
        present = {c.strip() for c in reader.fieldnames}
        missing = REQUIRED_COLUMNS - present
        if missing:
            raise ValueError(f"Missing required columns in {path.name}: {sorted(missing)}")
        for rownum, row in enumerate(reader, start=2):
            yield rownum, row


def canonicalize_row(
    *,
    broker: str,
    account_id: str,
    source_file_name: str,
    source_row_number: int,
    row: dict,
) -> CanonicalTrade:
    side = normalize_side(row.get("trade_type", ""))
    quantity = parse_decimal(row.get("quantity"), "quantity")
    price = parse_decimal(row.get("price"), "price")
    signed_quantity = quantity if side == "BUY" else -quantity
    trade_id = normalize_text(row.get("trade_id"))
    if not trade_id:
        raise ValueError("Missing trade_id")

    execution_timestamp = parse_required_datetime(row.get("order_execution_time"))
    instrument_key = build_instrument_key(row)
    order_id = row.get("order_id", "").strip()
    order_execution_time = row.get("order_execution_time", "").strip()

    source_unique_key = build_source_unique_key(
        broker,
        account_id,
        trade_id,
        order_id,
        order_execution_time,
    )
    raw_payload = dict(row)
    raw_payload["source_unique_key"] = source_unique_key

    auction_value = row.get("auction")
    auction: Optional[bool]
    if auction_value is None or str(auction_value).strip() == "":
        auction = None
    else:
        auction = str(auction_value).strip().lower() in {"true", "1", "t", "yes"}

    symbol = normalize_text(row.get("symbol"))
    if not symbol:
        raise ValueError("Missing symbol")

    return CanonicalTrade(
        broker=broker,
        account_id=account_id,
        source_file_name=source_file_name,
        source_row_number=source_row_number,
        instrument_key=instrument_key,
        symbol=symbol,
        exchange=normalize_text(row.get("exchange")),
        segment=normalize_text(row.get("segment")),
        expiry_date=parse_optional_date(row.get("expiry_date")),
        trade_id=trade_id,
        order_id=normalize_text(row.get("order_id")),
        execution_timestamp=execution_timestamp,
        trade_date=parse_optional_date(row.get("trade_date")),
        side=side,
        signed_quantity=signed_quantity,
        quantity=quantity,
        price=price,
        auction=auction,
        isin=normalize_text(row.get("isin")),
        series=normalize_text(row.get("series")),
        raw_payload=raw_payload,
        source_unique_key=source_unique_key,
    )


def create_run(
    conn: psycopg.Connection,
    *,
    broker: str,
    account_id: str,
    run_label: Optional[str],
    files: list[str],
) -> int:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            INSERT INTO trade_ingestion_runs (broker, account_id, run_label, run_type, files_json)
            VALUES (%s, %s, %s, 'HISTORICAL_FILE_SYNC', %s::jsonb)
            RETURNING id
            """,
            (broker, account_id, run_label, json.dumps(files)),
        )
        row = cur.fetchone()
    conn.commit()
    return int(row["id"])


def complete_run(
    conn: psycopg.Connection,
    *,
    run_id: int,
    status: str,
    rows_seen: int,
    rows_inserted: int,
    rows_ignored_duplicate: int,
    error_text: Optional[str] = None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE trade_ingestion_runs
            SET completed_at = NOW(),
                status = %s,
                rows_seen = %s,
                rows_inserted = %s,
                rows_ignored_duplicate = %s,
                error_text = %s
            WHERE id = %s
            """,
            (status, rows_seen, rows_inserted, rows_ignored_duplicate, error_text, run_id),
        )
    conn.commit()


def insert_trade(conn: psycopg.Connection, trade: CanonicalTrade, run_id: int) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO raw_broker_trades (
                broker,
                account_id,
                source_type,
                source_file_name,
                source_row_number,
                source_unique_key,
                instrument_key,
                symbol,
                exchange,
                segment,
                expiry_date,
                trade_id,
                order_id,
                execution_timestamp,
                trade_date,
                side,
                signed_quantity,
                quantity,
                price,
                auction,
                isin,
                series,
                raw_payload,
                ingestion_run_id
            ) VALUES (
                %s, %s, 'historical_tradebook_csv', %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s
            )
            ON CONFLICT (source_unique_key) DO NOTHING
            """,
            (
                trade.broker,
                trade.account_id,
                trade.source_file_name,
                trade.source_row_number,
                trade.source_unique_key,
                trade.instrument_key,
                trade.symbol,
                trade.exchange,
                trade.segment,
                trade.expiry_date,
                trade.trade_id,
                trade.order_id,
                trade.execution_timestamp,
                trade.trade_date,
                trade.side,
                trade.signed_quantity,
                trade.quantity,
                trade.price,
                trade.auction,
                trade.isin,
                trade.series,
                json.dumps(trade.raw_payload),
                run_id,
            ),
        )
        inserted = cur.rowcount == 1
    return inserted


def main() -> int:
    args = parse_args()

    files = [str(Path(f).resolve()) for f in args.files]
    rows_seen = 0
    rows_inserted = 0
    rows_ignored_duplicate = 0

    try:
        with psycopg.connect(args.dsn) as conn:
            if args.create_schema:
                ensure_schema(conn)

            run_id = create_run(
                conn,
                broker=args.broker,
                account_id=args.account_id,
                run_label=args.run_label,
                files=files,
            )

            try:
                for file_path in files:
                    path = Path(file_path)
                    for rownum, row in iter_csv_rows(path):
                        rows_seen += 1
                        trade = canonicalize_row(
                            broker=args.broker,
                            account_id=args.account_id,
                            source_file_name=path.name,
                            source_row_number=rownum,
                            row=row,
                        )
                        inserted = insert_trade(conn, trade, run_id)
                        if inserted:
                            rows_inserted += 1
                        else:
                            rows_ignored_duplicate += 1
                    conn.commit()

                complete_run(
                    conn,
                    run_id=run_id,
                    status="SUCCESS",
                    rows_seen=rows_seen,
                    rows_inserted=rows_inserted,
                    rows_ignored_duplicate=rows_ignored_duplicate,
                )

            except Exception as exc:
                conn.rollback()
                complete_run(
                    conn,
                    run_id=run_id,
                    status="FAILED",
                    rows_seen=rows_seen,
                    rows_inserted=rows_inserted,
                    rows_ignored_duplicate=rows_ignored_duplicate,
                    error_text=str(exc),
                )
                raise

    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    print("SUCCESS: Historical tradebook sync completed")
    print(f"rows_seen={rows_seen}")
    print(f"rows_inserted={rows_inserted}")
    print(f"rows_ignored_duplicate={rows_ignored_duplicate}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
