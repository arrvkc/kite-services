# stop_execution_engine.py

from __future__ import annotations

import hashlib
import time
import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from kiteconnect import KiteConnect

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


@dataclass(frozen=True)
class StopExecutionConfig:
    retries: int = 3
    retry_base_delay_sec: float = 0.5
    product: str = "NRML"
    material_change_pct: float = 0.0005
    material_change_tick_multiple: float = 2.0


def build_stop_hash(trigger_price: float, limit_price: float, quantity: int) -> str:
    payload = f"{round(trigger_price, 8)}|{round(limit_price, 8)}|{int(quantity)}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def call_with_retries(func, *args, retries: int, base_delay_sec: float, **kwargs):
    last_exc = None
    for attempt in range(1, retries + 1):
        try:
            return func(*args, **kwargs)
        except Exception as exc:
            last_exc = exc
            if attempt >= retries:
                raise
            time.sleep(base_delay_sec * attempt)
    raise last_exc if last_exc else RuntimeError("Unexpected retry failure")


def extract_trigger_id(resp: Any) -> int:
    if isinstance(resp, dict):
        if "trigger_id" in resp:
            return int(resp["trigger_id"])
        data = resp.get("data")
        if isinstance(data, dict) and "trigger_id" in data:
            return int(data["trigger_id"])
    return int(resp)


def extract_existing_trigger_price(gtt: Dict) -> Optional[float]:
    try:
        condition = gtt.get("condition") or {}
        trigger_values = condition.get("trigger_values") or []
        if not trigger_values:
            return None
        return float(trigger_values[0])
    except Exception:
        return None


def extract_existing_limit_price(gtt: Dict) -> Optional[float]:
    try:
        orders = gtt.get("orders") or []
        if not orders:
            return None
        return float(orders[0]["price"])
    except Exception:
        return None


def build_gtt_match_key_from_existing(gtt: Dict) -> Optional[Tuple[str, str, str, int, str]]:
    try:
        if gtt.get("status") != "active":
            return None

        condition = gtt.get("condition") or {}
        orders = gtt.get("orders") or []
        if not orders:
            return None

        order = orders[0]
        return (
            str(condition.get("tradingsymbol")),
            str(condition.get("exchange")),
            str(order.get("transaction_type")),
            int(order.get("quantity")),
            str(order.get("product")),
        )
    except Exception:
        return None


def build_gtt_match_key_from_plan(plan: Dict) -> Tuple[str, str, str, int, str]:
    return (
        str(plan["tradingsymbol"]),
        str(plan["exchange"]),
        str(plan["exit_transaction_type"]),
        int(plan["quantity"]),
        str(plan["product"]),
    )


def get_existing_active_gtt_map(kite: KiteConnect) -> Dict[Tuple[str, str, str, int, str], Dict]:
    gtt_map: Dict[Tuple[str, str, str, int, str], Dict] = {}
    for gtt in kite.get_gtts():
        key = build_gtt_match_key_from_existing(gtt)
        if key is not None:
            gtt_map[key] = gtt
    return gtt_map


def material_change_required(
    old_trigger: Optional[float],
    new_trigger: float,
    current_price_reference: float,
    tick_size: float,
    config: StopExecutionConfig,
) -> bool:
    if old_trigger is None:
        return True

    threshold = max(
        tick_size * config.material_change_tick_multiple,
        current_price_reference * config.material_change_pct,
    )
    return abs(new_trigger - old_trigger) >= threshold


def validate_plan(plan: Dict) -> None:
    required_keys = [
        "tradingsymbol",
        "exchange",
        "position_side",
        "exit_transaction_type",
        "quantity",
        "product",
        "trigger_price",
        "limit_price",
        "tick_size",
        "current_price_reference",
    ]
    missing = [k for k in required_keys if k not in plan]
    if missing:
        raise ValueError(f"Plan missing required keys: {missing}")

    if int(plan["quantity"]) <= 0:
        raise ValueError("Plan quantity must be > 0")

    trigger_price = float(plan["trigger_price"])
    limit_price = float(plan["limit_price"])
    current_price_reference = float(plan["current_price_reference"])
    side = str(plan["position_side"]).upper()

    if trigger_price <= 0 or limit_price <= 0:
        raise ValueError("Trigger and limit prices must be > 0")

    if side == "LONG":
        if limit_price > trigger_price:
            raise ValueError("For LONG positions, limit_price must be <= trigger_price")
        if trigger_price >= current_price_reference:
            raise ValueError("For LONG positions, trigger_price must be < current_price_reference")
    elif side == "SHORT":
        if limit_price < trigger_price:
            raise ValueError("For SHORT positions, limit_price must be >= trigger_price")
        if trigger_price <= current_price_reference:
            raise ValueError("For SHORT positions, trigger_price must be > current_price_reference")
    else:
        raise ValueError("position_side must be LONG or SHORT")


def enrich_plan_with_existing_state(
    plan: Dict,
    existing_gtt: Optional[Dict],
    config: StopExecutionConfig,
) -> Dict:
    enriched = dict(plan)

    previous_trigger_price = extract_existing_trigger_price(existing_gtt) if existing_gtt else None
    previous_limit_price = extract_existing_limit_price(existing_gtt) if existing_gtt else None

    enriched["existing_gtt_id"] = existing_gtt.get("id") if existing_gtt else None
    enriched["existing_trigger_price"] = previous_trigger_price
    enriched["existing_limit_price"] = previous_limit_price
    enriched["stop_hash"] = build_stop_hash(
        float(enriched["trigger_price"]),
        float(enriched["limit_price"]),
        int(enriched["quantity"]),
    )
    enriched["existing_stop_hash"] = (
        build_stop_hash(previous_trigger_price, previous_limit_price, int(enriched["quantity"]))
        if previous_trigger_price is not None and previous_limit_price is not None
        else None
    )
    enriched["update_required"] = material_change_required(
        old_trigger=previous_trigger_price,
        new_trigger=float(enriched["trigger_price"]),
        current_price_reference=float(enriched["current_price_reference"]),
        tick_size=float(enriched["tick_size"]),
        config=config,
    )

    return enriched


def execute_stoploss_plans(
    kite: KiteConnect,
    plans: List[Dict],
    dry_run: bool = True,
    config: StopExecutionConfig = StopExecutionConfig(),
) -> Dict:
    if not plans:
        return {
            "mode": "DRY_RUN" if dry_run else "LIVE",
            "results": [],
            "message": "No plans to execute.",
        }

    existing_gtt_map = get_existing_active_gtt_map(kite)
    results: List[Dict] = []

    for raw_plan in plans:
        validate_plan(raw_plan)

        key = build_gtt_match_key_from_plan(raw_plan)
        existing_gtt = existing_gtt_map.get(key)
        plan = enrich_plan_with_existing_state(raw_plan, existing_gtt, config)
        logger.info(
            f"[PLAN] {plan['tradingsymbol']} | Side={plan['position_side']} | "
            f"NewTrigger={plan['trigger_price']} | OldTrigger={plan.get('existing_trigger_price')}"
        )
        # GAP DETECTION (CRITICAL)
        ltp = float(plan["current_price_reference"])
        trigger = float(plan["trigger_price"])
        side = plan["position_side"]

        if (side == "LONG" and ltp <= trigger) or (side == "SHORT" and ltp >= trigger):
            logger.error(
                f"[GAP] {plan['tradingsymbol']} | Side={side} | LTP={ltp} crossed Trigger={trigger}"
            )
            results.append(
                {
                    "action": "GAP_EXIT_REQUIRED",
                    "trigger_id": int(existing_gtt["id"]) if existing_gtt else None,
                    "status": "CRITICAL",
                    "error": "Price already beyond stop. Market exit required.",
                    "plan": plan,
                }
            )
            continue

        if dry_run:
            if existing_gtt is None:
                action = "PLACE"
            elif plan.get("existing_stop_hash") == plan.get("stop_hash"):
                action = "UNCHANGED"
            else:
                action = "MODIFY"

            logger.info(
                f"[DRY_RUN] {plan['tradingsymbol']} | Action={action} | "
                f"NewTrigger={plan['trigger_price']} | "
                f"OldTrigger={plan.get('existing_trigger_price')} | "
                f"UpdateRequired={plan.get('update_required')}"
            )

            results.append(
                {
                    "action": action,
                    "trigger_id": int(existing_gtt["id"]) if existing_gtt else None,
                    "status": "SIMULATED",
                    "plan": plan,
                }
            )
            continue

        # STRICT UNCHANGED GUARD (HASH BASED)
        if existing_gtt and plan.get("existing_stop_hash") == plan.get("stop_hash"):
            logger.info(
                f"[SKIP] {plan['tradingsymbol']} | No change (hash match) | "
                f"Trigger={plan['trigger_price']}"
            )

            results.append(
                {
                    "action": "UNCHANGED",
                    "trigger_id": int(existing_gtt["id"]),
                    "status": "SKIPPED",
                    "plan": plan,
                }
            )
            continue

        kwargs = dict(
            trigger_type=kite.GTT_TYPE_SINGLE,
            tradingsymbol=plan["tradingsymbol"],
            exchange=plan["exchange"],
            trigger_values=[float(plan["trigger_price"])],
            last_price=float(plan["current_price_reference"]),
            orders=[
                {
                    "exchange": plan["exchange"],
                    "tradingsymbol": plan["tradingsymbol"],
                    "transaction_type": plan["exit_transaction_type"],
                    "quantity": int(plan["quantity"]),
                    "order_type": "LIMIT",
                    "product": plan["product"],
                    "price": float(plan["limit_price"]),
                }
            ],
        )

        try:
            if existing_gtt:
                logger.info(
                    f"[EXECUTE] {plan['tradingsymbol']} | Action=MODIFY | "
                    f"Trigger={plan['trigger_price']} | Limit={plan['limit_price']} | "
                    f"Qty={plan['quantity']} | ExistingGTT={existing_gtt['id']}"
                )
                resp = call_with_retries(
                    kite.modify_gtt,
                    trigger_id=int(existing_gtt["id"]),
                    retries=config.retries,
                    base_delay_sec=config.retry_base_delay_sec,
                    **kwargs,
                )
                action = "MODIFIED"
            else:
                logger.info(
                    f"[EXECUTE] {plan['tradingsymbol']} | Action=PLACE | "
                    f"Trigger={plan['trigger_price']} | Limit={plan['limit_price']} | "
                    f"Qty={plan['quantity']}"
                )
                resp = call_with_retries(
                    kite.place_gtt,
                    retries=config.retries,
                    base_delay_sec=config.retry_base_delay_sec,
                    **kwargs,
                )
                action = "PLACED"

            results.append(
                {
                    "action": action,
                    "trigger_id": extract_trigger_id(resp),
                    "status": "SUCCESS",
                    "plan": plan,
                }
            )
            logger.info(
                f"[SUCCESS] {plan['tradingsymbol']} | TriggerID={extract_trigger_id(resp)}"
            )
        except Exception as exc:
            results.append(
                {
                    "action": "ERROR",
                    "trigger_id": int(existing_gtt["id"]) if existing_gtt else None,
                    "status": "FAILED",
                    "error": str(exc),
                    "plan": plan,
                }
            )
            logger.error(
                f"[ERROR] {plan['tradingsymbol']} | {str(exc)}"
            )

    return {
        "mode": "DRY_RUN" if dry_run else "LIVE",
        "results": results,
    }