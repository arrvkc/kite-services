# stop_orchestrator.py

from __future__ import annotations

import sys
import csv
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from kiteconnect import KiteConnect

from kite_credentials_service import get_kite_credentials
from kite_market_data_service import get_all_futures_positions
from engines.stop_engine.stop_computation_engine import (
    StopComputationConfig,
    compute_deterministic_stop_eod,
    prepare_limit_order_from_trigger,
)
from engines.stop_engine.stop_execution_engine import (
    StopExecutionConfig,
    execute_stoploss_plans,
)
from engines.stop_engine.stop_execution_engine import get_existing_active_gtt_map, extract_existing_trigger_price
from engines.stop_engine.persistence.db import get_stop_db_session
from engines.stop_engine.persistence.stop_persistence_service import StopPersistenceService

logger = logging.getLogger(__name__)
VALID_CONTRACT_TYPES = {"near", "next", "far"}
MARKET_DATA_USER_ID = "XJ1877"


@dataclass(frozen=True)
class StopOrchestratorConfig:
    candle_days: int = 180
    candle_interval: str = "day"
    product: str = "NRML"
    computation: StopComputationConfig = StopComputationConfig()
    execution: StopExecutionConfig = StopExecutionConfig()


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
    market_data_kite = get_kite_client(MARKET_DATA_USER_ID)
    positions = get_all_futures_positions(
        user_id=user_id,
        exclude_zero_qty=True,
    )

    if symbol:
        symbol_upper = symbol.upper()
        positions = [p for p in positions if p["underlying"].upper() == symbol_upper]

    if not positions:
        return []

    ladder_map = get_market_expiry_ladder(market_data_kite, symbol=symbol)
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
    config: StopOrchestratorConfig,
) -> List[Dict]:
    now = datetime.now()
    # If market is open → ignore today candle
    if now.hour < 15 or (now.hour == 15 and now.minute < 30):
        to_dt = now.replace(hour=0, minute=0, second=0, microsecond=0)
    else:
        # After market close → include today
        to_dt = now
    from_dt = to_dt - timedelta(days=config.candle_days)
    raw = kite.historical_data(
        instrument_token=instrument_token,
        from_date=from_dt,
        to_date=to_dt,
        interval=config.candle_interval,
        continuous=False,
        oi=True,
    )
    candles = candles_to_dicts(raw)

    today = datetime.now().date()
    candles = [
        c for c in candles
        if c["date"].date() < today
    ]

    return candles


def build_execution_plans(
    user_id: str,
    contract_type: str = "near",
    symbol: Optional[str] = None,
    config: StopOrchestratorConfig = StopOrchestratorConfig(),
) -> List[Dict]:
    kite = get_kite_client(user_id)
    market_data_kite = get_kite_client(MARKET_DATA_USER_ID)

    existing_gtt_map = get_existing_active_gtt_map(kite)
    positions = find_positions(user_id=user_id, contract_type=contract_type, symbol=symbol)

    if not positions:
        return []

    plans: List[Dict] = []

    for position in positions:
        try:
            tradingsymbol = position["tradingsymbol"]
            exchange = position.get("exchange") or "NFO"
            quantity = int(position["quantity"])
            abs_quantity = abs(quantity)
            entry_price = float(position["avg_price"])
            side = "LONG" if quantity > 0 else "SHORT"
            exit_transaction_type = "SELL" if side == "LONG" else "BUY"
            tick_size = float(position.get("tick_size") or 0.05)
            instrument_token = int(position["instrument_token"])

            candles = get_completed_daily_candles(market_data_kite, instrument_token, config)

            key = (tradingsymbol, exchange, exit_transaction_type, abs_quantity, config.product)
            existing_gtt = existing_gtt_map.get(key)

            previous_trigger_price = None
            if existing_gtt:
                previous_trigger_price = extract_existing_trigger_price(existing_gtt)
            persisted_stop_state = None

            try:
                with get_stop_db_session() as session:
                    persistence = StopPersistenceService(session)

                    lifecycle = persistence.get_open_lifecycle(
                        account_id=user_id,
                        tradingsymbol=tradingsymbol,
                        side=side,
                    )

                    if lifecycle:
                        persisted_stop_state = persistence.get_latest_stop_state(
                            lifecycle_id=lifecycle.id
                        )

            except Exception as e:
                logger.warning(
                    "[PERSISTENCE UNAVAILABLE] %s | %s",
                    tradingsymbol,
                    str(e),
                )

            if previous_trigger_price is None and persisted_stop_state is not None:
                previous_trigger_price = float(persisted_stop_state.current_stop)

            now = datetime.now()

            # MARKET HOURS → DO NOT RECOMPUTE
            if now.hour < 15 or (now.hour == 15 and now.minute < 30):
                if previous_trigger_price is not None:
                    trigger_price = float(previous_trigger_price)
                    current_price_reference = float(candles[-1]["close"])

                    order = prepare_limit_order_from_trigger(
                        side=side,
                        trigger_price=trigger_price,
                        tick_size=tick_size,
                        current_price_reference=current_price_reference,
                    )

                    if side == "LONG":
                        per_unit_risk = max(0.0, entry_price - trigger_price)
                    else:
                        per_unit_risk = max(0.0, trigger_price - entry_price)

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
                            "product": config.product,
                            "entry_price": entry_price,
                            "tick_size": tick_size,
                            "trigger_price": trigger_price,
                            "limit_price": float(order["limit_price"]),
                            "current_price_reference": current_price_reference,
                            "per_unit_risk": round(per_unit_risk, 2),
                            "total_risk": round(total_risk, 2),
                            "details": {"mode": "FROZEN_INTRADAY"},
                        }
                    )

                    continue





            stop = compute_deterministic_stop_eod(
                candles=candles,
                side=side,
                tick_size=tick_size,
                entry_price=entry_price,
                previous_trigger_price=previous_trigger_price,
                config=config.computation,
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
                    "product": config.product,
                    "entry_price": entry_price,
                    "tick_size": tick_size,
                    "trigger_price": float(stop["trigger_price"]),
                    "limit_price": float(order["limit_price"]),
                    "current_price_reference": float(stop["current_price_reference"]),
                    "per_unit_risk": round(per_unit_risk, 2),
                    "total_risk": round(total_risk, 2),
                    "details": stop,
                }
            )
        except Exception as exc:
            logger.exception(
                "[SKIP] %s | reason=%s: %s",
                position.get("tradingsymbol", "UNKNOWN"),
                type(exc).__name__,
                exc,
            )
            continue

    return plans


def run_stop_orchestrator(
    user_id: str,
    contract_type: str = "near",
    symbol: Optional[str] = None,
    dry_run: bool = True,
    config: StopOrchestratorConfig = StopOrchestratorConfig(),
) -> Dict:
    kite = get_kite_client(user_id)
    plans = build_execution_plans(
        user_id=user_id,
        contract_type=contract_type,
        symbol=symbol,
        config=config,
    )

    if not plans:
        return {
            "mode": "DRY_RUN" if dry_run else "LIVE",
            "results": [],
            "message": f"No {contract_type}-month futures position found.",
        }

    return execute_stoploss_plans(
        kite=kite,
        plans=plans,
        dry_run=dry_run,
        config=config.execution,
    )


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


def print_help() -> None:
    print("Usage:")
    print("  python -m engines.stop_engine.stop_orchestrator <USER_ID> [contract_type] [symbol] [dry_run]")
    print("")
    print("Examples:")
    print("  python -m engines.stop_engine.stop_orchestrator OMK569")
    print("  python -m engines.stop_engine.stop_orchestrator OMK569 near HDFCBANK true")
    print("  python -m engines.stop_engine.stop_orchestrator OMK569 near HDFCBANK false")


def main() -> int:
    if len(sys.argv) < 2:
        print_help()
        return 1

    user_id = sys.argv[1]
    contract_type = "near"
    symbol = None
    dry_run = True

    if len(sys.argv) >= 3:
        arg2 = sys.argv[2].lower()
        if arg2 in VALID_CONTRACT_TYPES:
            contract_type = arg2
        elif arg2 in ("true", "false"):
            dry_run = arg2 == "true"
        else:
            symbol = sys.argv[2]

    if len(sys.argv) >= 4:
        arg3 = sys.argv[3].lower()
        if arg3 in ("true", "false"):
            dry_run = arg3 == "true"
        else:
            symbol = sys.argv[3]

    if len(sys.argv) >= 5:
        dry_run = sys.argv[4].lower() == "true"

    try:
        result = run_stop_orchestrator(
            user_id=user_id,
            contract_type=contract_type,
            symbol=symbol,
            dry_run=dry_run,
            config=StopOrchestratorConfig(),
        )

        if not result["results"]:
            print(result.get("message", "No results."))
            return 0

        rows = []
        for item in result["results"]:
            plan = item["plan"]
            details = plan.get("details", {})
            if plan["position_side"] == "LONG":
                dist_to_stop = plan["current_price_reference"] - plan["trigger_price"]
            else:
                dist_to_stop = plan["trigger_price"] - plan["current_price_reference"]

            dist_to_stop = max(0.0, dist_to_stop)

            rows.append([
                item["action"],
                str(item["trigger_id"]) if item["trigger_id"] is not None else "-",
                item["status"],
                plan["tradingsymbol"],
                plan["contract_type"],
                plan["position_side"],
                plan["exit_transaction_type"],
                str(plan["quantity"]),
                f"{plan['entry_price']:.2f}",
                f"{plan['current_price_reference']:.2f}",
                f"{plan['trigger_price']:.2f}",
                f"{plan['limit_price']:.2f}",
                f"{plan['per_unit_risk']:.2f}",
                f"{dist_to_stop:.2f}",
                f"{plan['total_risk']:.2f}",
                f"{plan.get('existing_trigger_price', 0.0):.2f}" if plan.get(
                    "existing_trigger_price") is not None else "-",
                f"{plan.get('existing_limit_price', 0.0):.2f}" if plan.get("existing_limit_price") is not None else "-",
                "YES" if plan.get("update_required") else "NO",
                f"{details.get('atr', 0.0):.2f}" if details.get("atr") is not None else "-",
                f"{details.get('atr_average', 0.0):.2f}" if details.get("atr_average") is not None else "-",
                f"{details.get('multiplier', 0.0):.2f}" if details.get("multiplier") is not None else "-",
                f"{details.get('raw_stop', 0.0):.2f}" if details.get("raw_stop") is not None else "-",
                details.get("final_source", "-"),
                details.get("stop_mode", "-"),
                f"{details.get('profit', 0.0):.2f}" if details.get("profit") is not None else "-",
                f"{details.get('min_distance', 0.0):.2f}" if details.get("min_distance") is not None else "-",
                details.get("min_rule", "-"),
                "YES" if details.get("min_distance_satisfied") else "NO",
            ])

        headers = [
            "Action", "Trigger ID", "Status", "Tradingsymbol", "Month", "Side", "Exit",
            "Qty", "Entry", "Close Ref", "Trigger", "Limit", "Risk/Unit",
            "Dist to Stop", "Total Risk", "Old Trigger", "Old Limit", "Update?",
            "ATR", "ATR Avg", "Mult", "Raw Stop", "Final Source",
            "Stop Mode", "Profit", "Min Dist", "Min Rule", "Min OK?",
        ]

        csv_file = f"stop_orchestrator_{user_id}_{contract_type}.csv"
        with open(csv_file, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            writer.writerows(rows)

        print(f"CSV written: {csv_file}")
        print("STOP ORCHESTRATOR")
        print_table(
            headers=[
                "Action",
                "Trigger ID",
                "Status",
                "Tradingsymbol",
                "Month",
                "Side",
                "Exit",
                "Qty",
                "Entry",
                "Close Ref",
                "Trigger",
                "Limit",
                "Risk/Unit",
                "Dist to Stop",
                "Total Risk",
                "Old Trigger",
                "Old Limit",
                "Update?",
                "ATR",
                "ATR Avg",
                "Mult",
                "Raw Stop",
                "Final Source",
                "Stop Mode",
                "Profit",
                "Min Dist",
                "Min Rule",
                "Min OK?",
            ],
            rows=rows,
        )
        return 0

    except Exception as exc:
        print(f"ERROR: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())