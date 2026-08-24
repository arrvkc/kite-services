from __future__ import annotations

import argparse
from datetime import date, datetime, time
from zoneinfo import ZoneInfo

import pandas as pd
from kiteconnect import KiteConnect

from services.kite_credentials_service import get_kite_credentials
from engines.strategy_deterministic_engine.adapters.trend_identifier_adapter import TrendIdentifierKiteAdapter
from engines.strategy_deterministic_engine.db.postgres import get_engine
from engines.strategy_deterministic_engine.db.upserts import upsert_contract_snapshot_rows


def to_ist_date(value) -> date:
    return pd.Timestamp(value).tz_convert("Asia/Kolkata").date()


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Sync contract-month snapshot for all F&O symbols into ATMS PostgreSQL."
    )
    parser.add_argument("user_id", help="Zerodha user id used to fetch Kite credentials")
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--symbols", help="Optional comma-separated symbols for testing")
    parser.add_argument(
        "--selection-date",
        help="Build the snapshot exactly as of this date (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Return non-zero if any requested symbol cannot be reconstructed.",
    )
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main() -> None:
    args = build_argument_parser().parse_args()
    selection_date = date.fromisoformat(args.selection_date) if args.selection_date else None
    explicit_asof = (
        datetime.combine(
            selection_date,
            time(hour=15, minute=30),
            tzinfo=ZoneInfo("Asia/Kolkata"),
        )
        if selection_date is not None
        else None
    )

    api_key, access_token = get_kite_credentials(args.user_id)
    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)

    adapter = TrendIdentifierKiteAdapter(kite=kite)
    symbols = (
        [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
        if args.symbols
        else adapter.get_fo_universe_symbols()
    )

    rows: list[dict] = []
    failures: list[dict] = []

    for index, symbol in enumerate(symbols, start=1):
        try:
            asof_time = explicit_asof
            if asof_time is None:
                asof_time = adapter._build_latest_payload(symbol).asof_time
            contract_info = adapter.get_contract_info_for_symbol(
                symbol,
                asof_time,
            )

            rows.append(
                {
                    "user_id": args.user_id,
                    "symbol": symbol,
                    "selection_date": selection_date or to_ist_date(asof_time),
                    "near_expiry": contract_info.near_expiry,
                    "next_expiry": contract_info.next_expiry,
                    "dte_near_month": contract_info.dte_near_month,
                    "next_month_available": contract_info.next_month_available,
                    "dte_next_month": contract_info.dte_next_month,
                }
            )

            print(f"[{index}/{len(symbols)}] OK {symbol}")

        except Exception as exc:
            safe_error = type(exc).__name__
            failures.append({"symbol": symbol, "error": safe_error})
            print(f"[{index}/{len(symbols)}] FAIL {symbol}: {safe_error}")

    if args.dry_run:
        print(f"DRY RUN: prepared_rows={len(rows)} failures={len(failures)}")
        if failures and args.strict:
            raise SystemExit(1)
        return

    if failures and args.strict:
        for failure in failures[:20]:
            print(f"FAILURE {failure['symbol']}: {failure['error']}")
        raise SystemExit(1)

    engine = get_engine()
    written = upsert_contract_snapshot_rows(engine, rows, batch_size=args.batch_size)

    print(f"UPSERTED contract_snapshot_fo_universe rows={written}")
    print(f"FAILURES count={len(failures)}")

    if failures:
        for failure in failures[:20]:
            print(f"FAILURE {failure['symbol']}: {failure['error']}")
        if args.strict:
            raise SystemExit(1)


if __name__ == "__main__":
    main()
