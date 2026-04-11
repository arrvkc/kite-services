#!/usr/bin/env python3
"""
Futures stop history what-if simulator.

Primary mode:
- User gives exact futures symbol and entry date
- Script fetches the close price for the entry date and uses that as entry price

Other supported modes:
1. Date-only entry:
   PYTHONPATH=.:services python engines/stop_engine/futures_stop_history_whatif.py HDFCBANK26APRFUT --entry-date 2026-03-27

2. Explicit entry price:
   PYTHONPATH=.:services python engines/stop_engine/futures_stop_history_whatif.py HDFCBANK26APRFUT --entry-date 2026-03-27 --entry-price 766.60

3. Explicit quantity:
   PYTHONPATH=.:services python engines/stop_engine/futures_stop_history_whatif.py HDFCBANK26APRFUT --entry-date 2026-03-27 --quantity 4400

4. End date override:
   PYTHONPATH=.:services python engines/stop_engine/futures_stop_history_whatif.py HDFCBANK26APRFUT --entry-date 2026-03-27 --end-date 2026-04-10

5. CSV-style output:
   PYTHONPATH=.:services python engines/stop_engine/futures_stop_history_whatif.py HDFCBANK26APRFUT --entry-date 2026-03-27 --format csv

Notes:
- This is a what-if / reconstructed path.
- It does not require a current position.
- It does not use raw_broker_trades.
- It does not persist state.
"""

from __future__ import annotations

import argparse
import csv
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, date
from typing import Dict, List, Optional

from kiteconnect import KiteConnect

from kite_credentials_service import get_kite_credentials
from engines.stop_engine.stop_computation_engine import (
    StopComputationConfig,
    compute_deterministic_stop_eod,
    prepare_limit_order_from_trigger,
)


@dataclass(frozen=True)
class FuturesWhatIfConfig:
    candle_interval: str = "day"
    warmup_days: int = 90
    default_quantity: int = 1
    exchange: str = "NFO"
    stop_config: StopComputationConfig = StopComputationConfig()


def parse_args() -> argparse.Namespace:
    examples = """
Examples:

  Use entry-date close as entry price:
    PYTHONPATH=.:services python engines/stop_engine/futures_stop_history_whatif.py HDFCBANK26APRFUT --entry-date 2026-03-27

  Use entry-date close and quantity 4400:
    PYTHONPATH=.:services python engines/stop_engine/futures_stop_history_whatif.py HDFCBANK26APRFUT --entry-date 2026-03-27 --quantity 4400

  Override entry price explicitly:
    PYTHONPATH=.:services python engines/stop_engine/futures_stop_history_whatif.py HDFCBANK26APRFUT --entry-date 2026-03-27 --entry-price 766.60 --quantity 4400

  Stop history only until a chosen end date:
    PYTHONPATH=.:services python engines/stop_engine/futures_stop_history_whatif.py HDFCBANK26APRFUT --entry-date 2026-03-27 --quantity 4400 --end-date 2026-04-10

  Show summary above the output:
    PYTHONPATH=.:services python engines/stop_engine/futures_stop_history_whatif.py HDFCBANK26APRFUT --entry-date 2026-03-27 --quantity 4400 --show-summary

  CSV output:
    PYTHONPATH=.:services python engines/stop_engine/futures_stop_history_whatif.py HDFCBANK26APRFUT --entry-date 2026-03-27 --quantity 4400 --format csv
"""
    parser = argparse.ArgumentParser(
        description="Futures stop history what-if simulator",
        epilog=examples,
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument("symbol", help="Exact futures symbol, e.g. HDFCBANK26APRFUT")
    parser.add_argument("--entry-date", required=True, help="Entry date in YYYY-MM-DD")
    parser.add_argument("--entry-price", type=float, default=None, help="Override entry price; if omitted, entry date close is used")
    parser.add_argument("--quantity", type=int, default=1, help="Scenario quantity; default 1")
    parser.add_argument("--side", choices=["LONG", "SHORT"], default="LONG", help="Scenario side; default LONG")
    parser.add_argument("--end-date", default=None, help="Optional end date in YYYY-MM-DD; default today")
    parser.add_argument("--tick-size", type=float, default=0.05, help="Tick size; default 0.05")
    parser.add_argument("--format", choices=["table", "csv"], default="table", help="Output format")
    parser.add_argument("--user-id", default="OMK569", help="User ID only for Kite credentials access")
    parser.add_argument("--show-summary", action="store_true", help="Print summary before rows")
    return parser.parse_args()


def get_kite_client(user_id: str) -> KiteConnect:
    api_key, access_token = get_kite_credentials(user_id)
    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    return kite


def parse_ymd(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def find_futures_instrument(kite: KiteConnect, symbol: str, exchange: str = "NFO") -> Dict:
    instruments = kite.instruments(exchange)
    matches = [
        inst for inst in instruments
        if str(inst.get("tradingsymbol") or "").upper() == symbol.upper()
    ]

    if not matches:
        raise ValueError(f"No {exchange} futures instrument found for symbol={symbol}")

    fut_matches = [m for m in matches if str(m.get("instrument_type") or "").upper() == "FUT"]
    if len(fut_matches) == 1:
        return fut_matches[0]
    if fut_matches:
        return fut_matches[0]
    return matches[0]


def candles_to_dicts(raw_candles: List[Dict]) -> List[Dict]:
    return [
        {
            "date": c["date"],
            "open": float(c["open"]),
            "high": float(c["high"]),
            "low": float(c["low"]),
            "close": float(c["close"]),
            "volume": c.get("volume"),
            "oi": c.get("oi"),
        }
        for c in raw_candles
    ]


def get_completed_daily_candles(
    kite: KiteConnect,
    instrument_token: int,
    entry_date: date,
    end_date: date,
    config: FuturesWhatIfConfig,
) -> List[Dict]:
    from_dt = datetime.combine(entry_date, datetime.min.time()) - timedelta(days=config.warmup_days)
    to_dt = datetime.combine(end_date, datetime.max.time())
    raw = kite.historical_data(
        instrument_token=instrument_token,
        from_date=from_dt,
        to_date=to_dt,
        interval=config.candle_interval,
        continuous=False,
        oi=True,
    )
    candles = candles_to_dicts(raw)
    if not candles:
        raise ValueError("No candles returned for the requested range")
    return candles


def resolve_entry_price(candles: List[Dict], entry_date: date, explicit_entry_price: Optional[float]) -> float:
    if explicit_entry_price is not None:
        if explicit_entry_price <= 0:
            raise ValueError("entry_price must be positive")
        return float(explicit_entry_price)

    for c in candles:
        if c["date"].date() == entry_date:
            return float(c["close"])

    raise ValueError(f"No candle found on entry_date={entry_date}; cannot infer entry price from close")


def print_table(headers: List[str], rows: List[List[str]]) -> None:
    widths = [len(str(h)) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(str(cell)))

    def fmt(values: List[str]) -> str:
        return " | ".join(str(v).ljust(widths[i]) for i, v in enumerate(values))

    separator = "-+-".join("-" * w for w in widths)
    print(fmt(headers))
    print(separator)
    for row in rows:
        print(fmt(row))


def emit_csv(headers: List[str], rows: List[List[str]]) -> None:
    writer = csv.writer(sys.stdout)
    writer.writerow(headers)
    writer.writerows(rows)


def main() -> int:
    args = parse_args()
    config = FuturesWhatIfConfig()

    entry_date = parse_ymd(args.entry_date)
    end_date = parse_ymd(args.end_date) if args.end_date else datetime.now().date()

    if end_date < entry_date:
        raise ValueError("end_date cannot be before entry_date")
    if args.quantity <= 0:
        raise ValueError("quantity must be positive")
    if args.tick_size <= 0:
        raise ValueError("tick_size must be positive")

    kite = get_kite_client(args.user_id)
    instrument = find_futures_instrument(kite, args.symbol, exchange=config.exchange)

    instrument_token = int(instrument["instrument_token"])
    tradingsymbol = str(instrument["tradingsymbol"])
    exchange = str(instrument["exchange"])

    candles = get_completed_daily_candles(
        kite=kite,
        instrument_token=instrument_token,
        entry_date=entry_date,
        end_date=end_date,
        config=config,
    )

    entry_price = resolve_entry_price(candles, entry_date, args.entry_price)

    previous_trigger_price: Optional[float] = None
    rows: List[List[str]] = []

    for i in range(len(candles)):
        partial = candles[: i + 1]
        candle_date = partial[-1]["date"].date()

        try:
            result = compute_deterministic_stop_eod(
                candles=partial,
                side=args.side,
                tick_size=args.tick_size,
                entry_price=entry_price,
                previous_trigger_price=previous_trigger_price,
                config=config.stop_config,
            )
        except ValueError:
            continue

        order = prepare_limit_order_from_trigger(
            side=args.side,
            trigger_price=float(result["trigger_price"]),
            tick_size=args.tick_size,
            current_price_reference=float(result["current_price_reference"]),
        )

        previous_trigger_price = float(result["trigger_price"])

        if candle_date < entry_date or candle_date > end_date:
            continue

        if args.side == "LONG":
            per_unit_risk = max(0.0, entry_price - float(result["trigger_price"]))
        else:
            per_unit_risk = max(0.0, float(result["trigger_price"]) - entry_price)

        total_risk = per_unit_risk * args.quantity

        rows.append([
            tradingsymbol,
            exchange,
            str(candle_date),
            args.side,
            str(args.quantity),
            f"{entry_price:.2f}",
            f"{result['current_price_reference']:.2f}",
            f"{result['trigger_price']:.2f}",
            f"{order['limit_price']:.2f}",
            f"{result['atr']:.2f}",
            f"{result['atr_average']:.2f}",
            f"{result['multiplier']:.2f}",
            f"{result['raw_stop']:.2f}",
            f"{per_unit_risk:.2f}",
            f"{total_risk:.2f}",
            "YES" if result["update_required"] else "NO",
        ])

    if not rows:
        print("No reconstructable what-if stop-history rows found.")
        return 0

    headers = [
        "Tradingsymbol",
        "Exchange",
        "Date",
        "Side",
        "Qty",
        "Entry Price",
        "Close Ref",
        "Trigger",
        "Limit",
        "ATR",
        "ATR Avg",
        "Mult",
        "Raw Stop",
        "Risk/Unit",
        "Total Risk",
        "Update?",
    ]

    if args.show_summary:
        print("FUTURES STOP HISTORY WHAT-IF")
        print(f"symbol={tradingsymbol}")
        print(f"exchange={exchange}")
        print(f"entry_date={entry_date}")
        print(f"entry_price={entry_price:.2f}")
        print(f"quantity={args.quantity}")
        print(f"side={args.side}")
        print(f"end_date={end_date}")
        print("")

    if args.format == "csv":
        emit_csv(headers, rows)
    else:
        print("FUTURES STOP HISTORY WHAT-IF")
        print_table(headers, rows)

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"ERROR: {exc}")
        raise SystemExit(1)
