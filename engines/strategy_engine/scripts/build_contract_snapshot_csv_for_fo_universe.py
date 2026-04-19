from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd
from kiteconnect import KiteConnect

from services.kite_credentials_service import get_kite_credentials
from engines.strategy_engine.adapters.trend_identifier_adapter import TrendIdentifierKiteAdapter


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build contract-month snapshot CSV for all F&O stock symbols."
    )
    parser.add_argument("user_id", help="Zerodha user id used to fetch Kite credentials")
    parser.add_argument(
        "--output",
        default="data/contract_snapshot_fo_universe.csv",
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

    rows = []
    failures = []

    for index, symbol in enumerate(symbols, start=1):
        try:
            latest_payload = adapter._build_latest_payload(symbol)
            contract_info = adapter.get_contract_info_for_symbol(symbol, latest_payload.asof_time)
            rows.append(
                {
                    "symbol": symbol,
                    "selection_date": pd.Timestamp(latest_payload.asof_time).tz_convert("Asia/Kolkata").date().isoformat(),
                    "near_expiry": contract_info.near_expiry.isoformat(),
                    "next_expiry": contract_info.next_expiry.isoformat() if contract_info.next_expiry is not None else None,
                    "dte_near_month": contract_info.dte_near_month,
                    "next_month_available": contract_info.next_month_available,
                    "dte_next_month": contract_info.dte_next_month,
                }
            )
            print(f"[{index}/{len(symbols)}] OK {symbol}")
        except Exception as exc:
            failures.append({"symbol": symbol, "error": str(exc)})
            print(f"[{index}/{len(symbols)}] FAIL {symbol}: {exc}")

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if rows:
        output_df = pd.DataFrame(rows).sort_values("symbol").reset_index(drop=True)
        output_df.to_csv(output_path, index=False)
        print(f"Wrote contract snapshot CSV: {output_path}")
    else:
        print("No contract rows were produced")

    if failures:
        failures_df = pd.DataFrame(failures)
        failure_path = output_path.with_name(output_path.stem + "_failures.csv")
        failures_df.to_csv(failure_path, index=False)
        print(f"Wrote failures CSV: {failure_path}")


if __name__ == "__main__":
    main()
