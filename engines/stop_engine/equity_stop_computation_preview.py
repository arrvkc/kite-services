#!/usr/bin/env python3
from __future__ import annotations

import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from kiteconnect import KiteConnect

from kite_credentials_service import get_kite_credentials
from engines.stop_engine.stop_computation_engine import (
    StopComputationConfig,
    compute_deterministic_stop_eod,
    prepare_limit_order_from_trigger,
)


@dataclass(frozen=True)
class EquityPreviewConfig:
    candle_days: int = 365
    candle_interval: str = "day"
    stop_config: StopComputationConfig = StopComputationConfig()


def get_kite_client(user_id: str) -> KiteConnect:
    api_key, access_token = get_kite_credentials(user_id)
    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    return kite


def get_all_equity_positions(kite: KiteConnect) -> List[Dict]:
    positions = kite.positions()
    net_positions = positions.get("net", []) or []
    out: List[Dict] = []

    for p in net_positions:
        exchange = str(p.get("exchange") or "").strip().upper()
        quantity = int(p.get("quantity") or 0)
        tradingsymbol = str(p.get("tradingsymbol") or "").strip()
        if quantity == 0 or not tradingsymbol:
            continue
        if exchange not in {"NSE", "BSE"}:
            continue
        out.append(p)

    seen = {(str(p.get("exchange") or "").strip().upper(), str(p.get("tradingsymbol") or "").strip()) for p in out}

    try:
        holdings = kite.holdings() or []
    except Exception:
        holdings = []

    for h in holdings:
        exchange = str(h.get("exchange") or "NSE").strip().upper()
        tradingsymbol = str(h.get("tradingsymbol") or "").strip()
        quantity = int(h.get("quantity") or 0) + int(h.get("t1_quantity") or 0)
        if quantity <= 0 or not tradingsymbol:
            continue
        key = (exchange, tradingsymbol)
        if key in seen:
            continue
        h = dict(h)
        h["quantity"] = quantity
        h["average_price"] = float(h.get("average_price") or 0.0)
        h["product"] = "HOLDING"
        out.append(h)

    return out


def filter_equity_positions(positions: List[Dict], symbol: Optional[str] = None) -> List[Dict]:
    if symbol:
        sym = symbol.upper()
        positions = [p for p in positions if str(p.get("tradingsymbol") or "").upper() == sym]
    return positions


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


def get_completed_daily_candles(kite: KiteConnect, instrument_token: int, config: EquityPreviewConfig) -> List[Dict]:
    to_dt = datetime.now()
    from_dt = to_dt - timedelta(days=config.candle_days)
    raw = kite.historical_data(
        instrument_token=instrument_token,
        from_date=from_dt,
        to_date=to_dt,
        interval=config.candle_interval,
        continuous=False,
        oi=False,
    )
    return candles_to_dicts(raw)


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


def build_preview_rows(user_id: str, symbol: Optional[str] = None, config: EquityPreviewConfig = EquityPreviewConfig()) -> List[List[str]]:
    kite = get_kite_client(user_id)
    positions = filter_equity_positions(get_all_equity_positions(kite), symbol=symbol)

    rows: List[List[str]] = []

    for position in positions:
        quantity = int(position.get("quantity") or 0)
        if quantity <= 0:
            continue

        tradingsymbol = str(position.get("tradingsymbol") or "").strip()
        exchange = str(position.get("exchange") or "NSE").strip().upper()
        instrument_token = position.get("instrument_token")
        if instrument_token is None or not tradingsymbol:
            continue

        entry_price = float(position.get("avg_price") or position.get("average_price") or 0.0)
        if entry_price <= 0:
            continue

        tick_size = float(position.get("tick_size") or 0.05)
        candles = get_completed_daily_candles(kite, int(instrument_token), config)

        stop = compute_deterministic_stop_eod(
            candles=candles,
            side="LONG",
            tick_size=tick_size,
            entry_price=entry_price,
            previous_trigger_price=None,
            config=config.stop_config,
        )

        order = prepare_limit_order_from_trigger(
            side="LONG",
            trigger_price=float(stop["trigger_price"]),
            tick_size=tick_size,
            current_price_reference=float(stop["current_price_reference"]),
        )

        per_unit_risk = max(0.0, entry_price - float(stop["trigger_price"]))
        total_risk = per_unit_risk * quantity

        rows.append([
            tradingsymbol,
            exchange,
            str(quantity),
            f"{entry_price:.2f}",
            f"{stop['current_price_reference']:.2f}",
            f"{stop['trigger_price']:.2f}",
            f"{order['limit_price']:.2f}",
            f"{stop['atr']:.2f}",
            f"{stop['atr_average']:.2f}",
            f"{stop['multiplier']:.2f}",
            f"{stop['raw_stop']:.2f}",
            f"{per_unit_risk:.2f}",
            f"{total_risk:.2f}",
            "YES" if stop["update_required"] else "NO",
        ])

    return rows


def print_help() -> None:
    print("Usage:")
    print("  python equity_stop_computation_preview.py <USER_ID> [SYMBOL]")


def main() -> int:
    if len(sys.argv) < 2:
        print_help()
        return 1

    user_id = sys.argv[1]
    symbol = sys.argv[2] if len(sys.argv) >= 3 else None

    try:
        rows = build_preview_rows(user_id=user_id, symbol=symbol)
        if not rows:
            print("No matching long equity positions found.")
            return 0

        print("EQUITY STOP COMPUTATION PREVIEW - REAL DATA - EOD COMPUTATION")
        print_table(
            headers=[
                "Tradingsymbol",
                "Exchange",
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
            ],
            rows=rows,
        )
        return 0
    except Exception as exc:
        print(f"ERROR: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
