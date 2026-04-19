from __future__ import annotations

import argparse
from pathlib import Path
from typing import List

import pandas as pd
from kiteconnect import KiteConnect

from services.kite_credentials_service import get_kite_credentials
from engines.strategy_engine.adapters.trend_identifier_adapter import TrendIdentifierKiteAdapter
from engines.trend_identifier.trend_identifier.runners.equity_trend_history_runner import (
    EquityTrendHistoryRunner,
)


class UniverseBuildError(Exception):
    pass


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build combined 5-day Trend Identifier history CSV for all F&O stock symbols."
    )
    parser.add_argument("user_id", help="Zerodha user id used to fetch Kite credentials")
    parser.add_argument(
        "--history-days",
        type=int,
        default=5,
        help="Number of completed daily Trend Identifier history rows per symbol",
    )
    parser.add_argument(
        "--output",
        default="data/trend_history_fo_universe.csv",
        help="Output CSV path",
    )
    return parser


def main() -> None:
    parser = build_argument_parser()
    args = parser.parse_args()

    api_key, access_token = get_kite_credentials(args.user_id)
    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)

    adapter = TrendIdentifierKiteAdapter(kite=kite)
    symbols = adapter.get_fo_stock_symbols()
    history_runner = EquityTrendHistoryRunner(kite=kite, exchange="NSE")

    output_rows = []
    failures = []

    for index, symbol in enumerate(symbols, start=1):
        try:
            result = history_runner.build_history_for_symbol(symbol=symbol, history_days=args.history_days)
            history_df = result.history.copy()
            history_df["symbol"] = result.symbol
            history_df["exchange"] = result.exchange
            history_df["tradingsymbol"] = result.tradingsymbol
            history_df["instrument_token"] = result.instrument_token
            output_rows.append(history_df)
            print(f"[{index}/{len(symbols)}] OK {symbol}")
        except Exception as exc:
            failures.append({"symbol": symbol, "error": str(exc)})
            print(f"[{index}/{len(symbols)}] FAIL {symbol}: {exc}")

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if output_rows:
        combined_df = pd.concat(output_rows, ignore_index=True)
        preferred_columns = [
            "symbol",
            "date",
            "close",
            "label",
            "confidence",
            "aggregate_score",
            "internal_state",
            "exchange",
            "tradingsymbol",
            "instrument_token",
        ]
        existing_columns = [column for column in preferred_columns if column in combined_df.columns]
        combined_df = combined_df[existing_columns].sort_values(["symbol", "date"]).reset_index(drop=True)
        combined_df.to_csv(output_path, index=False)
        print(f"Wrote trend history CSV: {output_path}")
    else:
        print("No trend history rows were produced")

    if failures:
        failures_df = pd.DataFrame(failures)
        failure_path = output_path.with_name(output_path.stem + "_failures.csv")
        failures_df.to_csv(failure_path, index=False)
        print(f"Wrote failures CSV: {failure_path}")


if __name__ == "__main__":
    main()
