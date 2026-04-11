#!/usr/bin/env python3
"""
Dry run stop computation using real Kite data, but EOD-compliant.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

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
class RealDataDryRunConfig:
    candle_days: int = 180
    candle_interval: str = "day"
    use_existing_gtt_trigger: bool = True
    stop_config: StopComputationConfig = StopComputationConfig()


def get_kite_client(user_id: str) -> KiteConnect:
    api_key, access_token = get_kite_credentials(user_id)
    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    return kite


def get_nfo_futures_instruments(kite: KiteConnect) -> List[Dict]:
    return [i for i in kite.instruments("NFO") if i.get("instrument_type") == "FUT"]


def get_market_expiry_ladder(
    kite: KiteConnect,
    symbol: Optional[str] = None,
) -> Dict[str, List[Dict]]:
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


def find_positions(
    user_id: str,
    contract_type: str,
    symbol: Optional[str] = None,
) -> List[Dict]:
    if contract_type not in VALID_CONTRACT_TYPES:
        raise ValueError("contract_type must be one of: near, next, far")

    kite = get_kite_client(user_id)
    positions = get_all_futures_positions(
        user_id=user_id,
        exclude_zero_qty=True,
    )

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
    config: RealDataDryRunConfig,
) -> List[Dict]:
    to_dt = datetime.now()
    from_dt = to_dt - timedelta(days=config.candle_days)
    raw = kite.historical_data(
        instrument_token=instrument_token,
        from_date=from_dt,
        to_date=to_dt,
        interval=config.candle_interval,
        continuous=False,
        oi=True,
    )
    return candles_to_dicts(raw)


def extract_existing_trigger_price(gtt: Dict) -> Optional[float]:
    try:
        condition = gtt.get("condition") or {}
        trigger_values = condition.get("trigger_values") or []
        if not trigger_values:
            return None
        return float(trigger_values[0])
    except Exception:
        return None


def build_gtt_match_key_from_existing(gtt: Dict) -> Optional[Tuple]:
    try:
        if gtt.get("status") != "active":
            return None
        condition = gtt.get("condition") or {}
        orders = gtt.get("orders") or []
        if not orders:
            return None
        order = orders[0]
        return (
            condition.get("tradingsymbol"),
            condition.get("exchange"),
            order.get("transaction_type"),
            int(order.get("quantity")),
            order.get("product"),
        )
    except Exception:
        return None


def get_existing_active_gtt_map(kite: KiteConnect) -> Dict[Tuple, Dict]:
    gtt_map: Dict[Tuple, Dict] = {}
    for gtt in kite.get_gtts():
        key = build_gtt_match_key_from_existing(gtt)
        if key is not None:
            gtt_map[key] = gtt
    return gtt_map


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


def build_dry_run_plans(
    user_id: str,
    contract_type: str = "near",
    symbol: Optional[str] = None,
    config: RealDataDryRunConfig = RealDataDryRunConfig(),
) -> List[Dict]:
    kite = get_kite_client(user_id)
    positions = find_positions(user_id=user_id, contract_type=contract_type, symbol=symbol)

    if not positions:
        return []

    existing_gtt_map = get_existing_active_gtt_map(kite) if config.use_existing_gtt_trigger else {}
    plans: List[Dict] = []

    for position in positions:
        tradingsymbol = position["tradingsymbol"]
        exchange = position.get("exchange") or "NFO"
        quantity = int(position["quantity"])
        abs_quantity = abs(quantity)
        entry_price = float(position["avg_price"])
        side = "LONG" if quantity > 0 else "SHORT"
        exit_transaction_type = "SELL" if side == "LONG" else "BUY"
        tick_size = float(position.get("tick_size") or 0.05)
        instrument_token = int(position["instrument_token"])

        previous_trigger_price = None
        existing_gtt_id = None

        if config.use_existing_gtt_trigger:
            key = (tradingsymbol, exchange, exit_transaction_type, abs_quantity, "NRML")
            existing_gtt = existing_gtt_map.get(key)
            if existing_gtt:
                existing_gtt_id = existing_gtt.get("id")
                previous_trigger_price = extract_existing_trigger_price(existing_gtt)

        candles = get_completed_daily_candles(kite, instrument_token, config)

        stop = compute_deterministic_stop_eod(
            candles=candles,
            side=side,
            tick_size=tick_size,
            entry_price=entry_price,
            previous_trigger_price=previous_trigger_price,
            config=config.stop_config,
        )

        order = prepare_limit_order_from_trigger(
            side=side,
            trigger_price=float(stop["trigger_price"]),
            tick_size=tick_size,
            current_price_reference=float(stop["current_price_reference"]),
        )

        if side == "LONG":
            per_unit_risk = max(0.0, entry_price - float(stop["trigger_price"]))
        else:
            per_unit_risk = max(0.0, float(stop["trigger_price"]) - entry_price)

        total_risk = per_unit_risk * abs_quantity

        plans.append(
            {
                "user_id": user_id,
                "contract_type": contract_type,
                "symbol_filter": symbol,
                "tradingsymbol": tradingsymbol,
                "exchange": exchange,
                "position_side": side,
                "exit_transaction_type": exit_transaction_type,
                "quantity": abs_quantity,
                "entry_price": entry_price,
                "tick_size": tick_size,
                "existing_gtt_id": existing_gtt_id,
                "existing_trigger_price": previous_trigger_price,
                "trigger_price": float(stop["trigger_price"]),
                "limit_price": float(order["limit_price"]),
                "per_unit_risk": round(per_unit_risk, 2),
                "total_risk": round(total_risk, 2),
                "details": stop,
            }
        )

    return plans


def print_help() -> None:
    print("Usage:")
    print("  python dry_run_deterministic_stop_real_data.py <USER_ID> [contract_type] [symbol]")


def main() -> int:
    if len(sys.argv) < 2:
        print_help()
        return 1

    user_id = sys.argv[1]
    contract_type = "near"
    symbol = None

    if len(sys.argv) >= 3:
        arg2 = sys.argv[2].lower()
        if arg2 in VALID_CONTRACT_TYPES:
            contract_type = arg2
        else:
            symbol = sys.argv[2]

    if len(sys.argv) >= 4:
        symbol = sys.argv[3]

    try:
        plans = build_dry_run_plans(
            user_id=user_id,
            contract_type=contract_type,
            symbol=symbol,
            config=RealDataDryRunConfig(),
        )

        if not plans:
            print(f"No {contract_type}-month futures position found.")
            return 0

        rows = []
        for plan in plans:
            d = plan["details"]
            rows.append([
                plan["tradingsymbol"],
                plan["contract_type"],
                plan["position_side"],
                plan["exit_transaction_type"],
                str(plan["quantity"]),
                f"{plan['entry_price']:.2f}",
                f"{d['current_price_reference']:.2f}",
                f"{plan['trigger_price']:.2f}",
                f"{plan['limit_price']:.2f}",
                f"{d['atr']:.2f}",
                f"{d['atr_average']:.2f}",
                f"{d['multiplier']:.2f}",
                f"{d['raw_stop']:.2f}",
                f"{plan['per_unit_risk']:.2f}",
                f"{plan['total_risk']:.2f}",
                "YES" if d["update_required"] else "NO",
            ])

        print("DRY RUN - REAL DATA - EOD COMPUTATION")
        print_table(
            headers=[
                "Tradingsymbol",
                "Month",
                "Side",
                "Exit",
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
