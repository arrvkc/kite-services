from __future__ import annotations

import argparse
import sys
from pathlib import Path

from kiteconnect import KiteConnect

REPO_ROOT = Path(__file__).resolve().parents[4]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from engines.trend_identifier.trend_identifier.runners.equity_trend_history_runner import (  # noqa: E402
    EquityTrendHistoryRunner,
)
from services.kite_credentials_service import get_kite_credentials  # noqa: E402


def build_kite_client(user_id: str) -> KiteConnect:
    api_key, access_token = get_kite_credentials(user_id)
    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    return kite


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run Trend Identifier history for one equity using Kite Connect data."
    )
    parser.add_argument("user_id", help="Zerodha user id used by services.kite_credentials_service.py")
    parser.add_argument("symbol", help="Stock symbol, for example CIPLA")
    parser.add_argument("--exchange", default="NSE", help="Exchange to resolve instruments from. Default: NSE")
    parser.add_argument("--history-days", type=int, required=True, help="Number of trading-day history points to produce")
    parser.add_argument("--daily-lookback-days", type=int, default=900, help="Daily candle lookback window. Default: 900")
    parser.add_argument("--hourly-lookback-days", type=int, default=120, help="Hourly candle lookback window. Default: 120")
    parser.add_argument("--csv-out", default="", help="Optional CSV output path")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    kite = build_kite_client(args.user_id)
    runner = EquityTrendHistoryRunner(kite=kite, exchange=args.exchange)

    result = runner.build_history_for_symbol(
        symbol=args.symbol,
        history_days=args.history_days,
        daily_lookback_days=args.daily_lookback_days,
        hourly_lookback_days=args.hourly_lookback_days,
    )

    print(f"SYMBOL: {result.symbol}")
    print(f"EXCHANGE: {result.exchange}")
    print(f"TRADINGSYMBOL: {result.tradingsymbol}")
    print(f"INSTRUMENT_TOKEN: {result.instrument_token}")
    print()
    print(result.history.to_string(index=False))

    if args.csv_out:
        out_path = Path(args.csv_out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        result.history.to_csv(out_path, index=False)
        print()
        print(f"Saved CSV: {out_path}")


if __name__ == "__main__":
    main()
