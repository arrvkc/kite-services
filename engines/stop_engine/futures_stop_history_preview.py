#!/usr/bin/env python3
"""
Reconstruct daily stop history from current lifecycle start date to today.

Run like:
PYTHONPATH=.:services python engines/stop_engine/stop_history_dry_run.py OMK569 next HDFCBANK

What it does:
- selects the current open position using real broker positions
- finds the current open lifecycle start from raw_broker_trades
- fetches completed daily candles from Kite
- recomputes the EOD stop day-by-day from lifecycle start to today
- prints one row per day

This is a reconstructed theoretical stop history.
It is not persisted historical accepted stop state.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timedelta, date
from decimal import Decimal
from typing import Dict, List, Optional, Tuple

import psycopg
from kiteconnect import KiteConnect

from kite_credentials_service import get_kite_credentials
from kite_market_data_service import get_all_futures_positions
from engines.stop_engine.stop_computation_engine import (
    StopComputationConfig,
    compute_deterministic_stop_eod,
    prepare_limit_order_from_trigger,
)

VALID_CONTRACT_TYPES = {"near", "next", "far"}


@dataclass(frozen=True)
class StopHistoryConfig:
    candle_days: int = 365
    candle_interval: str = "day"
    dsn: str = "postgresql://postgres:postgres@localhost:5432/trades"
    broker: str = "zerodha"
    stop_config: StopComputationConfig = StopComputationConfig()


def get_kite_client(user_id: str) -> KiteConnect:
    api_key, access_token = get_kite_credentials(user_id)
    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    return kite


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


def find_positions(user_id: str, contract_type: str, symbol: Optional[str] = None) -> List[Dict]:
    if contract_type not in VALID_CONTRACT_TYPES:
        raise ValueError("contract_type must be one of: near, next, far")

    kite = get_kite_client(user_id)
    positions = get_all_futures_positions(user_id=user_id, exclude_zero_qty=True)

    if symbol:
        symbol_upper = symbol.upper()
        positions = [p for p in positions if p["underlying"].upper() == symbol_upper]

    if not positions:
        return []

    ladder_map = get_market_expiry_ladder(kite, symbol=symbol)
    idx = {"near": 0, "next": 1, "far": 2}[contract_type]

    selected_positions: List[Dict] = []
    positions_by_underlying: Dict[str, List[Dict]] = {}
    for p in positions:
        positions_by_underlying.setdefault(p["underlying"], []).append(p)

    for underlying, held_positions in positions_by_underlying.items():
        ladder = ladder_map.get(underlying, [])
        if idx >= len(ladder):
            continue
        selected_contract = ladder[idx]["tradingsymbol"]
        matched = [p for p in held_positions if p["tradingsymbol"] == selected_contract]
        selected_positions.extend(matched)

    return selected_positions


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
    start_date: date,
    config: StopHistoryConfig,
) -> List[Dict]:
    from_dt = datetime.combine(start_date, datetime.min.time()) - timedelta(days=60)
    to_dt = datetime.now()
    raw = kite.historical_data(
        instrument_token=instrument_token,
        from_date=from_dt,
        to_date=to_dt,
        interval=config.candle_interval,
        continuous=False,
        oi=True,
    )
    return candles_to_dicts(raw)


def map_position_exchange_to_db_exchange(exchange: str) -> str:
    ex = (exchange or "").strip().upper()
    if ex in {"", "NFO"}:
        return "NSE"
    if ex == "BFO":
        return "BSE"
    return ex


def build_db_instrument_key_from_position(position: Dict) -> str:
    exchange = map_position_exchange_to_db_exchange(str(position.get("exchange") or ""))
    tradingsymbol = str(position.get("tradingsymbol") or "").strip()
    if not tradingsymbol:
        raise ValueError("Missing tradingsymbol in position")
    return f"{exchange}|FO|{tradingsymbol}"


def fetch_current_lifecycle_start(
    dsn: str,
    broker: str,
    account_id: str,
    instrument_key: str,
) -> Tuple[datetime, Decimal]:
    sql = """
        SELECT execution_timestamp, signed_quantity, trade_id, order_id
        FROM raw_broker_trades
        WHERE broker = %s
          AND account_id = %s
          AND instrument_key = %s
        ORDER BY execution_timestamp ASC, trade_id ASC, order_id ASC
    """

    rows: List[Tuple[datetime, Decimal]] = []
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (broker, account_id, instrument_key))
            rows = cur.fetchall()

    if not rows:
        raise ValueError(f"No raw trades found for instrument_key={instrument_key}")

    net = Decimal("0")
    lifecycle_start: Optional[datetime] = None

    for execution_timestamp, signed_quantity, trade_id, order_id in rows:
        prev_net = net
        net += Decimal(str(signed_quantity))

        if prev_net == 0 and net != 0:
            lifecycle_start = execution_timestamp

    if net == 0:
        raise ValueError(f"Instrument {instrument_key} is not currently open in raw trade history")

    if lifecycle_start is None:
        raise ValueError(f"Could not determine lifecycle start for {instrument_key}")

    return lifecycle_start, net


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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Reconstruct daily stop history from current lifecycle start")
    parser.add_argument("user_id", help="Zerodha user ID, e.g. OMK569")
    parser.add_argument("contract_type", nargs="?", default="near", choices=["near", "next", "far"])
    parser.add_argument("symbol", nargs="?", default=None)
    parser.add_argument("--dsn", default="postgresql://postgres:postgres@localhost:5432/trades")
    parser.add_argument("--broker", default="zerodha")
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    kite = get_kite_client(args.user_id)
    positions = find_positions(args.user_id, args.contract_type, args.symbol)

    if not positions:
        print(f"No {args.contract_type}-month futures position found.")
        return 0

    rows_to_print: List[List[str]] = []

    for position in positions:
        instrument_key = build_db_instrument_key_from_position(position)
        lifecycle_start, net_qty = fetch_current_lifecycle_start(
            dsn=args.dsn,
            broker=args.broker,
            account_id=args.user_id,
            instrument_key=instrument_key,
        )

        side = "LONG" if int(position["quantity"]) > 0 else "SHORT"
        tick_size = float(position.get("tick_size") or 0.05)
        entry_price = float(position["avg_price"])
        instrument_token = int(position["instrument_token"])

        candles = get_completed_daily_candles(
            kite=kite,
            instrument_token=instrument_token,
            start_date=lifecycle_start.date(),
            config=StopHistoryConfig(dsn=args.dsn, broker=args.broker),
        )

        previous_trigger_price: Optional[float] = None

        for i in range(len(candles)):
            partial = candles[: i + 1]
            candle_date = partial[-1]["date"].date()

            try:
                result = compute_deterministic_stop_eod(
                    candles=partial,
                    side=side,
                    tick_size=tick_size,
                    entry_price=entry_price,
                    previous_trigger_price=previous_trigger_price,
                    config=StopHistoryConfig().stop_config,
                )
            except ValueError:
                continue

            order = prepare_limit_order_from_trigger(
                side=side,
                trigger_price=float(result["trigger_price"]),
                tick_size=tick_size,
                current_price_reference=float(result["current_price_reference"]),
            )

            previous_trigger_price = float(result["trigger_price"])

            if candle_date < lifecycle_start.date():
                continue

            if side == "LONG":
                per_unit_risk = max(0.0, entry_price - float(result["trigger_price"]))
            else:
                per_unit_risk = max(0.0, float(result["trigger_price"]) - entry_price)

            total_risk = per_unit_risk * abs(int(position["quantity"]))

            rows_to_print.append([
                position["tradingsymbol"],
                str(candle_date),
                side,
                str(abs(int(position["quantity"]))),
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

    if not rows_to_print:
        print("No reconstructable stop-history rows found.")
        return 0

    print("RECONSTRUCTED STOP HISTORY - REAL DATA - EOD COMPUTATION")
    print_table(
        headers=[
            "Tradingsymbol",
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
        ],
        rows=rows_to_print,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
