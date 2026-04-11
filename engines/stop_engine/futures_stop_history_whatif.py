#!/usr/bin/env python3
"""
Futures stop history what-if simulator.

Primary mode:
- User gives underlying symbol and contract type
- Script resolves near / next / far from the NFO expiry ladder
- Script fetches the close price for the entry date and uses that as entry price

Supported modes:
1. Underlying + contract type + entry date:
   PYTHONPATH=.:services python engines/stop_engine/futures_stop_history_whatif.py HDFCBANK --contract-type near --entry-date 2026-03-27

2. Explicit entry price:
   PYTHONPATH=.:services python engines/stop_engine/futures_stop_history_whatif.py HDFCBANK --contract-type near --entry-date 2026-03-27 --entry-price 766.60

3. Explicit lots:
   PYTHONPATH=.:services python engines/stop_engine/futures_stop_history_whatif.py HDFCBANK --contract-type near --entry-date 2026-03-27 --lots 8

4. End date override:
   PYTHONPATH=.:services python engines/stop_engine/futures_stop_history_whatif.py HDFCBANK --contract-type near --entry-date 2026-03-27 --end-date 2026-04-10

5. CSV-style output:
   PYTHONPATH=.:services python engines/stop_engine/futures_stop_history_whatif.py HDFCBANK --contract-type near --entry-date 2026-03-27 --format csv

6. Exact futures symbol override:
   PYTHONPATH=.:services python engines/stop_engine/futures_stop_history_whatif.py HDFCBANK --futures-symbol HDFCBANK26APRFUT --entry-date 2026-03-27

Notes:
- This is a what-if / reconstructed path.
- It does not require a current position.
- It does not use raw_broker_trades.
- It does not persist state.
- Default quantity is 1 lot.
- User input is in lots, not units.
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

VALID_CONTRACT_TYPES = {"near", "next", "far"}


@dataclass(frozen=True)
class FuturesWhatIfConfig:
    candle_interval: str = "day"
    warmup_days: int = 90
    exchange: str = "NFO"
    stop_config: StopComputationConfig = StopComputationConfig()


def parse_args() -> argparse.Namespace:
    examples = """
Examples:

  Use entry-date close as entry price:
    PYTHONPATH=.:services python engines/stop_engine/futures_stop_history_whatif.py HDFCBANK --contract-type near --entry-date 2026-03-27

  Use entry-date close and 8 lots:
    PYTHONPATH=.:services python engines/stop_engine/futures_stop_history_whatif.py HDFCBANK --contract-type near --entry-date 2026-03-27 --lots 8

  Override entry price explicitly:
    PYTHONPATH=.:services python engines/stop_engine/futures_stop_history_whatif.py HDFCBANK --contract-type near --entry-date 2026-03-27 --entry-price 766.60 --lots 8

  Stop history only until a chosen end date:
    PYTHONPATH=.:services python engines/stop_engine/futures_stop_history_whatif.py HDFCBANK --contract-type near --entry-date 2026-03-27 --lots 8 --end-date 2026-04-10

  Show summary above the output:
    PYTHONPATH=.:services python engines/stop_engine/futures_stop_history_whatif.py HDFCBANK --contract-type near --entry-date 2026-03-27 --lots 8 --show-summary

  CSV output:
    PYTHONPATH=.:services python engines/stop_engine/futures_stop_history_whatif.py HDFCBANK --contract-type near --entry-date 2026-03-27 --lots 8 --format csv

  Exact futures symbol override:
    PYTHONPATH=.:services python engines/stop_engine/futures_stop_history_whatif.py HDFCBANK --futures-symbol HDFCBANK26APRFUT --entry-date 2026-03-27
"""
    parser = argparse.ArgumentParser(
        description="Futures stop history what-if simulator",
        epilog=examples,
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument("symbol", help="Underlying symbol, e.g. HDFCBANK")
    parser.add_argument("--contract-type", default="near", choices=["near", "next", "far"], help="Contract selection from expiry ladder")
    parser.add_argument("--futures-symbol", default=None, help="Optional exact futures tradingsymbol override, e.g. HDFCBANK26APRFUT")
    parser.add_argument("--entry-date", required=True, help="Entry date in YYYY-MM-DD")
    parser.add_argument("--entry-price", type=float, default=None, help="Override entry price; if omitted, entry date close is used")
    parser.add_argument("--lots", type=int, default=1, help="Scenario lots; default 1 lot")
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


def get_nfo_futures_instruments(kite: KiteConnect) -> List[Dict]:
    return [i for i in kite.instruments("NFO") if i.get("instrument_type") == "FUT"]


def get_market_expiry_ladder(kite: KiteConnect, symbol: Optional[str] = None) -> Dict[str, List[Dict]]:
    today = datetime.now().date()
    instruments = get_nfo_futures_instruments(kite)
    grouped: Dict[str, List[Dict]] = {}

    for inst in instruments:
        expiry = inst.get("expiry")
        underlying = inst.get("name")
        if not expiry or not underlying:
            continue
        if expiry < today:
            continue
        if symbol and underlying.upper() != symbol.upper():
            continue
        grouped.setdefault(underlying, []).append(inst)

    for underlying in grouped:
        grouped[underlying] = sorted(grouped[underlying], key=lambda x: x["expiry"])

    return grouped


def resolve_futures_instrument(
    kite: KiteConnect,
    underlying: str,
    contract_type: str,
    futures_symbol: Optional[str] = None,
) -> Dict:
    instruments = get_nfo_futures_instruments(kite)

    if futures_symbol:
        matches = [
            inst for inst in instruments
            if str(inst.get("tradingsymbol") or "").upper() == futures_symbol.upper()
        ]
        if not matches:
            raise ValueError(f"No NFO futures instrument found for futures_symbol={futures_symbol}")
        return matches[0]

    if contract_type not in VALID_CONTRACT_TYPES:
        raise ValueError("contract_type must be one of: near, next, far")

    ladder_map = get_market_expiry_ladder(kite, symbol=underlying)
    ladder = ladder_map.get(underlying.upper(), [])
    idx = {"near": 0, "next": 1, "far": 2}[contract_type]

    if idx >= len(ladder):
        raise ValueError(f"No {contract_type}-month futures contract found for underlying={underlying}")

    return ladder[idx]


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
    if args.lots <= 0:
        raise ValueError("lots must be positive")
    if args.tick_size <= 0:
        raise ValueError("tick_size must be positive")

    kite = get_kite_client(args.user_id)
    instrument = resolve_futures_instrument(
        kite=kite,
        underlying=args.symbol.upper(),
        contract_type=args.contract_type,
        futures_symbol=args.futures_symbol,
    )

    instrument_token = int(instrument["instrument_token"])
    tradingsymbol = str(instrument["tradingsymbol"])
    exchange = "NFO"
    lot_size = int(instrument.get("lot_size") or 1)
    quantity_units = args.lots * lot_size

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

        total_risk = per_unit_risk * quantity_units

        rows.append([
            tradingsymbol,
            exchange,
            str(candle_date),
            args.side,
            str(args.lots),
            str(quantity_units),
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
        "Lots",
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
        print(f"underlying={args.symbol.upper()}")
        print(f"resolved_tradingsymbol={tradingsymbol}")
        print(f"exchange={exchange}")
        print(f"contract_type={args.contract_type}")
        print(f"entry_date={entry_date}")
        print(f"entry_price={entry_price:.2f}")
        print(f"lots={args.lots}")
        print(f"lot_size={lot_size}")
        print(f"quantity_units={quantity_units}")
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
