from __future__ import annotations

import argparse
import math
from datetime import date

import pandas as pd
from kiteconnect import KiteConnect

from services.kite_credentials_service import get_kite_credentials
from engines.strategy_deterministic_engine.adapters.trend_identifier_adapter import TrendIdentifierKiteAdapter
from engines.trend_identifier.trend_identifier.runners.equity_trend_history_runner import (
    EquityTrendHistoryRunner,
)
from engines.strategy_deterministic_engine.db.postgres import get_engine
from engines.strategy_deterministic_engine.db.upserts import upsert_trend_history_rows


def clean_value(value):
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if pd.isna(value):
        return None
    return value


def to_date(value) -> date:
    return pd.Timestamp(value).date()


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Sync Trend Identifier history for all F&O symbols into ATMS PostgreSQL."
    )
    parser.add_argument("user_id", help="Zerodha user id used to fetch Kite credentials")
    parser.add_argument("--history-days", type=int, default=5)
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--symbols", help="Optional comma-separated symbols for testing")
    parser.add_argument("--dry-run", action="store_true")
    return parser


def build_symbol_rows(args, result) -> list[dict]:
    history_df = result.history.copy()
    symbol_rows: list[dict] = []

    for _, row in history_df.iterrows():
        symbol_rows.append(
            {
                "user_id": args.user_id,
                "symbol": result.symbol,
                "trade_date": to_date(row["date"]),
                "close": clean_value(row.get("close")),
                "label": clean_value(row.get("label")),
                "confidence": clean_value(row.get("confidence")),
                "aggregate_score": clean_value(row.get("aggregate_score")),
                "internal_state": clean_value(row.get("internal_state")),
                "exchange": result.exchange,
                "tradingsymbol": result.tradingsymbol,
                "instrument_token": result.instrument_token,
            }
        )

    return symbol_rows


def main() -> None:
    args = build_argument_parser().parse_args()

    api_key, access_token = get_kite_credentials(args.user_id)
    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)

    adapter = TrendIdentifierKiteAdapter(kite=kite)
    symbols = (
        [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
        if args.symbols
        else adapter.get_fo_universe_symbols()
    )

    history_runner = EquityTrendHistoryRunner(kite=kite, exchange="NSE")

    engine = None if args.dry_run else get_engine()
    total_rows = 0
    failures: list[dict] = []

    for index, symbol in enumerate(symbols, start=1):
        try:
            result = history_runner.build_history_for_symbol(
                symbol=symbol,
                history_days=args.history_days,
            )

            symbol_rows = build_symbol_rows(args, result)

            if args.dry_run:
                total_rows += len(symbol_rows)
                print(f"[{index}/{len(symbols)}] DRY OK {symbol} rows={len(symbol_rows)}", flush=True)
            else:
                written = upsert_trend_history_rows(
                    engine,
                    symbol_rows,
                    batch_size=args.batch_size,
                )
                total_rows += written
                print(f"[{index}/{len(symbols)}] OK {symbol} committed_rows={written}", flush=True)

        except Exception as exc:
            failures.append({"symbol": symbol, "error": str(exc)})
            print(f"[{index}/{len(symbols)}] FAIL {symbol}: {exc}", flush=True)

    if args.dry_run:
        print(f"DRY RUN: prepared_rows={total_rows} failures={len(failures)}")
    else:
        print(f"UPSERTED trend_history_fo_universe rows={total_rows}")
        print(f"FAILURES count={len(failures)}")

    if failures:
        for failure in failures[:20]:
            print(f"FAILURE {failure['symbol']}: {failure['error']}")


if __name__ == "__main__":
    main()
