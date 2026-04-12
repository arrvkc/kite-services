from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from kiteconnect import KiteConnect

# Make repo root importable when this script is run directly.
REPO_ROOT = Path(__file__).resolve().parents[4]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from engines.trend_identifier.trend_identifier.runners.equity_trend_runner import (  # noqa: E402
    EquityTrendRunner,
    summarize_results,
)
from services.kite_credentials_service import get_kite_credentials  # noqa: E402


def build_kite_client(user_id: str) -> KiteConnect:
    api_key, access_token = get_kite_credentials(user_id)
    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    return kite


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the Trend Identifier for one or more equities using Kite Connect data."
    )
    parser.add_argument(
        "user_id",
        help="Zerodha user id used by services.kite_credentials_service.py",
    )
    parser.add_argument(
        "symbols",
        nargs="+",
        help="One or more stock symbols, for example POWERINDIA RELIANCE INFY",
    )
    parser.add_argument(
        "--exchange",
        default="NSE",
        help="Exchange to resolve instruments from. Default: NSE",
    )
    parser.add_argument(
        "--daily-lookback-days",
        type=int,
        default=900,
        help="Daily candle lookback window. Default: 900",
    )
    parser.add_argument(
        "--hourly-lookback-days",
        type=int,
        default=120,
        help="Hourly candle lookback window. Default: 120",
    )
    parser.add_argument(
        "--pretty",
        action="store_true",
        help="Pretty-print full JSON result",
    )
    parser.add_argument(
        "--summary-only",
        action="store_true",
        help="Print only the compact summary table",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    kite = build_kite_client(args.user_id)
    runner = EquityTrendRunner(kite=kite, exchange=args.exchange)

    results = runner.run_for_symbols(
        symbols=args.symbols,
        daily_lookback_days=args.daily_lookback_days,
        hourly_lookback_days=args.hourly_lookback_days,
    )

    summary_df = summarize_results(results)

    if args.pretty and not args.summary_only:
        print(json.dumps(results, indent=2, default=str))
        print()

    print(summary_df.to_string(index=False))

    if len(results) == 1 and not args.summary_only:
        print()
        payload = results[0]["payload"]
        print(f"SYMBOL: {results[0]['symbol']}")
        print(f"LABEL: {payload['label']}")
        print(f"CONFIDENCE: {payload['confidence']}")
        print(f"AGGREGATE_SCORE: {payload['aggregate_score']}")
        print(f"INTERNAL_STATE: {payload['internal_state']}")


if __name__ == "__main__":
    main()
