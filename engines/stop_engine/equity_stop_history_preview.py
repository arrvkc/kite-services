#!/usr/bin/env python3
from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timedelta, date
from decimal import Decimal
from typing import Dict, List, Optional, Tuple

import psycopg
from kiteconnect import KiteConnect

from kite_credentials_service import get_kite_credentials
from engines.stop_engine.stop_computation_engine import (
    StopComputationConfig,
    compute_deterministic_stop_eod,
    prepare_limit_order_from_trigger,
)


@dataclass(frozen=True)
class EquityHistoryConfig:
    candle_days: int = 730
    candle_interval: str = "day"
    dsn: str = "postgresql://postgres:postgres@localhost:5432/trades"
    broker: str = "zerodha"
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
        if quantity <= 0 or not tradingsymbol:
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


def get_completed_daily_candles(kite: KiteConnect, instrument_token: int, start_date: date, config: EquityHistoryConfig) -> List[Dict]:
    from_dt = datetime.combine(start_date, datetime.min.time()) - timedelta(days=60)
    to_dt = datetime.now()
    raw = kite.historical_data(
        instrument_token=instrument_token,
        from_date=from_dt,
        to_date=to_dt,
        interval=config.candle_interval,
        continuous=False,
        oi=False,
    )
    return candles_to_dicts(raw)


def build_db_instrument_key(exchange: str, tradingsymbol: str) -> str:
    ex = (exchange or "NSE").strip().upper()
    if ex not in {"NSE", "BSE"}:
        ex = "NSE"
    return f"{ex}|{ex}|{tradingsymbol}"


def fetch_current_lifecycle_start(dsn: str, broker: str, account_id: str, instrument_key: str) -> Tuple[datetime, Decimal]:
    sql = """
        SELECT execution_timestamp, signed_quantity, trade_id, order_id
        FROM raw_broker_trades
        WHERE broker = %s
          AND account_id = %s
          AND instrument_key = %s
        ORDER BY execution_timestamp ASC, trade_id ASC, order_id ASC
    """

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
    parser = argparse.ArgumentParser(description="Reconstruct daily equity stop history from current lifecycle start")
    parser.add_argument("user_id")
    parser.add_argument("symbol", nargs="?", default=None)
    parser.add_argument("--dsn", default="postgresql://postgres:postgres@localhost:5432/trades")
    parser.add_argument("--broker", default="zerodha")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    kite = get_kite_client(args.user_id)
    positions = get_all_equity_positions(kite)

    if args.symbol:
        symbol = args.symbol.upper()
        positions = [p for p in positions if str(p.get("tradingsymbol") or "").upper() == symbol]

    if not positions:
        print("No matching long equity positions found.")
        return 0

    rows_to_print: List[List[str]] = []

    for position in positions:
        quantity = int(position.get("quantity") or 0)
        if quantity <= 0:
            continue

        exchange = str(position.get("exchange") or "NSE").strip().upper()
        tradingsymbol = str(position.get("tradingsymbol") or "").strip()
        instrument_token = position.get("instrument_token")
        if instrument_token is None or not tradingsymbol:
            continue

        entry_price = float(position.get("avg_price") or position.get("average_price") or 0.0)
        if entry_price <= 0:
            continue

        instrument_key = build_db_instrument_key(exchange, tradingsymbol)
        lifecycle_start, net_qty = fetch_current_lifecycle_start(
            dsn=args.dsn,
            broker=args.broker,
            account_id=args.user_id,
            instrument_key=instrument_key,
        )

        tick_size = float(position.get("tick_size") or 0.05)
        candles = get_completed_daily_candles(
            kite=kite,
            instrument_token=int(instrument_token),
            start_date=lifecycle_start.date(),
            config=EquityHistoryConfig(dsn=args.dsn, broker=args.broker),
        )

        previous_trigger_price: Optional[float] = None

        for i in range(len(candles)):
            partial = candles[: i + 1]
            candle_date = partial[-1]["date"].date()

            try:
                result = compute_deterministic_stop_eod(
                    candles=partial,
                    side="LONG",
                    tick_size=tick_size,
                    entry_price=entry_price,
                    previous_trigger_price=previous_trigger_price,
                    config=EquityHistoryConfig().stop_config,
                )
            except ValueError:
                continue

            order = prepare_limit_order_from_trigger(
                side="LONG",
                trigger_price=float(result["trigger_price"]),
                tick_size=tick_size,
                current_price_reference=float(result["current_price_reference"]),
            )

            previous_trigger_price = float(result["trigger_price"])

            if candle_date < lifecycle_start.date():
                continue

            per_unit_risk = max(0.0, entry_price - float(result["trigger_price"]))
            total_risk = per_unit_risk * quantity

            rows_to_print.append([
                tradingsymbol,
                str(candle_date),
                "LONG",
                str(quantity),
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
        print("No reconstructable equity stop-history rows found.")
        return 0

    print("RECONSTRUCTED EQUITY STOP HISTORY - REAL DATA - EOD COMPUTATION")
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
