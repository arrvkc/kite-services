#!/usr/bin/env python3
"""
Print the last N completed daily candles and verify the swing low.

Usage:
  PYTHONPATH=.:services python verify_swing_low.py OMK569 ASTRAL
  PYTHONPATH=.:services python verify_swing_low.py OMK569 ASTRAL 5
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta
from typing import Dict, List

from kiteconnect import KiteConnect

from kite_credentials_service import get_kite_credentials
from kite_market_data_service import get_all_futures_positions


def get_kite_client(user_id: str) -> KiteConnect:
    api_key, access_token = get_kite_credentials(user_id)
    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    return kite


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


def get_fut_position_for_symbol(user_id: str, symbol: str) -> Dict:
    positions = get_all_futures_positions(user_id=user_id, exclude_zero_qty=True)
    symbol = symbol.upper()

    matches = [p for p in positions if p.get("underlying", "").upper() == symbol]
    if not matches:
        raise ValueError(f"No open futures position found for symbol: {symbol}")

    if len(matches) > 1:
        print("Multiple futures positions found. Using the first one:")
        for p in matches:
            print(f"  {p['tradingsymbol']} qty={p['quantity']}")
    return matches[0]


def get_completed_daily_candles(
    kite: KiteConnect,
    instrument_token: int,
    candle_days: int = 90,
) -> List[Dict]:
    now = datetime.now()

    # During market hours, exclude today's partial daily candle.
    if now.hour < 15 or (now.hour == 15 and now.minute < 30):
        to_dt = now.replace(hour=0, minute=0, second=0, microsecond=0)
    else:
        to_dt = now

    from_dt = to_dt - timedelta(days=candle_days)

    raw = kite.historical_data(
        instrument_token=instrument_token,
        from_date=from_dt,
        to_date=to_dt,
        interval="day",
        continuous=False,
        oi=True,
    )
    return candles_to_dicts(raw)


def print_table(headers: List[str], rows: List[List[str]]) -> None:
    widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(str(cell)))

    def fmt(row: List[str]) -> str:
        return " | ".join(str(v).ljust(widths[i]) for i, v in enumerate(row))

    separator = "-+-".join("-" * w for w in widths)
    print(fmt(headers))
    print(separator)
    for row in rows:
        print(fmt(row))


def main() -> int:
    if len(sys.argv) < 3:
        print("Usage: PYTHONPATH=.:services python verify_swing_low.py <USER_ID> <SYMBOL> [LOOKBACK]")
        return 1

    user_id = sys.argv[1]
    symbol = sys.argv[2]
    lookback = int(sys.argv[3]) if len(sys.argv) >= 4 else 5

    if lookback <= 0:
        raise ValueError("LOOKBACK must be > 0")

    kite = get_kite_client(user_id)
    position = get_fut_position_for_symbol(user_id, symbol)

    tradingsymbol = position["tradingsymbol"]
    instrument_token = int(position["instrument_token"])

    candles = get_completed_daily_candles(kite, instrument_token, candle_days=90)
    if len(candles) < lookback:
        raise ValueError(f"Not enough candles. Need {lookback}, got {len(candles)}")

    recent = candles[-lookback:]
    swing_low = min(c["low"] for c in recent)

    rows = []
    for c in recent:
        marker = "<-- swing low" if abs(c["low"] - swing_low) < 1e-9 else ""
        rows.append([
            str(c["date"]),
            f"{c['open']:.2f}",
            f"{c['high']:.2f}",
            f"{c['low']:.2f}",
            f"{c['close']:.2f}",
            marker,
        ])

    print(f"Tradingsymbol: {tradingsymbol}")
    print(f"Lookback: {lookback}")
    print()
    print_table(
        headers=["Date", "Open", "High", "Low", "Close", "Note"],
        rows=rows,
    )
    print()
    print(f"Swing Low ({lookback} candles) = {swing_low:.2f}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())