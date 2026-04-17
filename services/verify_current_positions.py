#!/usr/bin/env python3
"""
Verify current open positions from local PostgreSQL trade ingestion data
against KiteConnect net positions for a Zerodha user.

What it does:
- Reads DB-derived open positions from raw_broker_trades
- Excludes expired contracts on the DB side
- Fetches Kite positions() and uses the "net" list
- Normalizes both sides into a comparable instrument key:
    exchange|segment|tradingsymbol
- Compares quantities and reports:
    - MATCH
    - DB_ONLY
    - KITE_ONLY
    - QTY_MISMATCH

Assumptions:
- Local PostgreSQL is reachable through the provided DSN
- Kite credentials are retrievable via get_kite_credentials()
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from decimal import Decimal
from typing import Dict, List, Tuple

import psycopg
from kiteconnect import KiteConnect

from kite_credentials_service import get_kite_credentials


@dataclass
class PositionRow:
    instrument_key: str
    quantity: Decimal


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify DB open positions against KiteConnect positions")
    parser.add_argument("user_id", help="Zerodha user ID, e.g. OMK569")
    parser.add_argument(
        "--dsn",
        default="postgresql://postgres:postgres@localhost:5432/trades",
        help="PostgreSQL DSN",
    )
    parser.add_argument(
        "--broker",
        default="zerodha",
        help="Broker name stored in raw_broker_trades",
    )
    parser.add_argument(
        "--print-matches",
        action="store_true",
        help="Also print exact matches",
    )
    parser.add_argument(
        "--host",
        default="root@eajee.in",
        help="SSH host for Kite credential lookup",
    )
    parser.add_argument(
        "--db-container",
        default="postgres",
        help="Remote Postgres Docker container name for Kite credential lookup",
    )
    parser.add_argument(
        "--db-user",
        default="atms",
        help="Remote Postgres DB user for Kite credential lookup",
    )
    parser.add_argument(
        "--db-name",
        default="atms",
        help="Remote Postgres DB name for Kite credential lookup",
    )
    return parser.parse_args()


def fetch_db_open_positions(dsn: str, broker: str, account_id: str) -> Dict[str, Decimal]:
    sql = """
        SELECT
            instrument_key,
            SUM(signed_quantity) AS net_quantity
        FROM raw_broker_trades
        WHERE broker = %s
          AND account_id = %s
        GROUP BY instrument_key, expiry_date
        HAVING SUM(signed_quantity) <> 0
           AND (expiry_date IS NULL OR expiry_date >= CURRENT_DATE)
        ORDER BY instrument_key
    """

    out: Dict[str, Decimal] = {}
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (broker, account_id))
            for instrument_key, net_quantity in cur.fetchall():
                qty = Decimal(str(net_quantity))
                out[instrument_key] = out.get(instrument_key, Decimal("0")) + qty
    # Remove any accidental zeros after aggregation
    return {k: v for k, v in out.items() if v != 0}


def normalize_kite_exchange_and_segment(position: dict) -> tuple[str, str]:
    exchange = str(position.get("exchange") or "").strip().upper()

    if exchange == "NFO":
        return "NSE", "FO"
    if exchange == "BFO":
        return "BSE", "FO"
    if exchange == "CDS":
        return "CDS", "CDS"
    if exchange == "MCX":
        return "MCX", "MCX"

    return exchange, exchange


def build_kite_instrument_key(position: dict) -> str:
    exchange, segment = normalize_kite_exchange_and_segment(position)
    tradingsymbol = str(position.get("tradingsymbol") or "").strip()
    return f"{exchange}|{segment}|{tradingsymbol}"


def fetch_kite_open_positions(
    user_id: str,
    host: str,
    db_container: str,
    db_user: str,
    db_name: str,
) -> Dict[str, Decimal]:
    api_key, access_token = get_kite_credentials(
        user_id=user_id,
        host=host,
        db_container=db_container,
        db_user=db_user,
        db_name=db_name,
    )

    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)

    positions = kite.positions()
    net_positions = positions.get("net", [])

    out: Dict[str, Decimal] = {}
    for row in net_positions:
        qty = Decimal(str(row.get("quantity", 0)))
        if qty == 0:
            continue
        instrument_key = build_kite_instrument_key(row)
        out[instrument_key] = out.get(instrument_key, Decimal("0")) + qty

    return out


def compare_positions(
    db_positions: Dict[str, Decimal],
    kite_positions: Dict[str, Decimal],
) -> Tuple[List[Tuple[str, str, Decimal, Decimal]], Dict[str, int]]:
    all_keys = sorted(set(db_positions) | set(kite_positions))
    results: List[Tuple[str, str, Decimal, Decimal]] = []
    counts = {
        "MATCH": 0,
        "DB_ONLY": 0,
        "KITE_ONLY": 0,
        "QTY_MISMATCH": 0,
    }

    for key in all_keys:
        db_qty = db_positions.get(key, Decimal("0"))
        kite_qty = kite_positions.get(key, Decimal("0"))

        if key in db_positions and key in kite_positions:
            if db_qty == kite_qty:
                status = "MATCH"
            else:
                status = "QTY_MISMATCH"
        elif key in db_positions:
            status = "DB_ONLY"
        else:
            status = "KITE_ONLY"

        counts[status] += 1
        results.append((status, key, db_qty, kite_qty))

    return results, counts


def main() -> int:
    args = parse_args()

    try:
        db_positions = fetch_db_open_positions(
            dsn=args.dsn,
            broker=args.broker,
            account_id=args.user_id,
        )
    except Exception as e:
        print(f"ERROR: Failed to fetch DB positions: {e}")
        return 1

    try:
        kite_positions = fetch_kite_open_positions(
            user_id=args.user_id,
            host=args.host,
            db_container=args.db_container,
            db_user=args.db_user,
            db_name=args.db_name,
        )
    except Exception as e:
        print(f"ERROR: Failed to fetch Kite positions: {e}")
        return 2

    results, counts = compare_positions(db_positions, kite_positions)

    print("SUCCESS: Position verification completed")
    print(f"db_open_position_count={len(db_positions)}")
    print(f"kite_open_position_count={len(kite_positions)}")
    print(
        "summary="
        f"MATCH:{counts['MATCH']} "
        f"DB_ONLY:{counts['DB_ONLY']} "
        f"KITE_ONLY:{counts['KITE_ONLY']} "
        f"QTY_MISMATCH:{counts['QTY_MISMATCH']}"
    )

    for status, key, db_qty, kite_qty in results:
        if status == "MATCH" and not args.print_matches:
            continue
        print(f"{status:<12} {key:<45} db={db_qty} kite={kite_qty}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
