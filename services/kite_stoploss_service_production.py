import sys
import time
import hashlib
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from kiteconnect import KiteConnect

from kite_credentials_service import get_kite_credentials
from kite_market_data_service import get_all_futures_positions


VALID_CONTRACT_TYPES = {"near", "next", "far"}


@dataclass(frozen=True)
class StopLossConfig:
    candle_days: int = 180
    candle_interval: str = "day"
    atr_period: int = 14
    atr_average_window: int = 20
    base_multiplier: float = 2.5
    volatility_spike_ratio: float = 2.0
    volatility_spike_multiplier: float = 1.3
    swing_lookback: int = 5
    min_distance_pct: float = 0.005
    min_distance_atr_multiple: float = 0.5
    spread_threshold_pct: float = 0.0015
    spread_threshold_tick_multiple: float = 1.5
    base_buffer_pct: float = 0.001
    material_change_pct: float = 0.0005
    material_change_tick_multiple: float = 2.0
    retries: int = 3
    retry_base_delay_sec: float = 0.5
    use_depth_quotes: bool = True


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


def true_range(high: float, low: float, prev_close: float) -> float:
    return max(
        high - low,
        abs(high - prev_close),
        abs(low - prev_close),
    )


def compute_true_ranges(candles: List[Dict]) -> List[float]:
    if len(candles) < 2:
        return []

    trs = []
    for i in range(1, len(candles)):
        trs.append(
            true_range(
                float(candles[i]["high"]),
                float(candles[i]["low"]),
                float(candles[i - 1]["close"]),
            )
        )
    return trs


def compute_atr(candles: List[Dict], period: int) -> float:
    trs = compute_true_ranges(candles)
    if len(trs) < period:
        raise ValueError(f"Need at least {period + 1} candles to compute ATR({period})")
    recent = trs[-period:]
    return sum(recent) / len(recent)


def compute_atr_average(candles: List[Dict], atr_period: int, avg_window: int) -> float:
    min_candles = atr_period + avg_window
    if len(candles) < min_candles:
        raise ValueError(
            f"Need at least {min_candles} candles to compute ATR average "
            f"(atr_period={atr_period}, avg_window={avg_window})"
        )

    atr_values: List[float] = []
    for end_idx in range(atr_period + 1, len(candles) + 1):
        atr_values.append(compute_atr(candles[:end_idx], atr_period))

    recent = atr_values[-avg_window:]
    return sum(recent) / len(recent)


def round_to_tick(price: float, tick_size: float) -> float:
    if tick_size <= 0:
        return round(price, 2)
    return round(round(price / tick_size) * tick_size, 8)


def floor_to_tick(price: float, tick_size: float) -> float:
    if tick_size <= 0:
        return round(price, 2)
    units = int(price / tick_size)
    return round(units * tick_size, 8)


def ceil_to_tick(price: float, tick_size: float) -> float:
    if tick_size <= 0:
        return round(price, 2)
    units = int(price / tick_size)
    exact = units * tick_size
    if abs(exact - price) < 1e-12:
        return round(exact, 8)
    return round((units + 1) * tick_size, 8)


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


def get_daily_candles(
    kite: KiteConnect,
    instrument_token: int,
    config: StopLossConfig,
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
    candles = candles_to_dicts(raw)
    min_needed = max(config.atr_period + config.atr_average_window, config.swing_lookback + 1)
    if len(candles) < min_needed:
        raise ValueError(
            f"Not enough candles returned for stop calculation; need at least {min_needed}, got {len(candles)}"
        )
    return candles


def get_quote_snapshot(kite: KiteConnect, exchange: str, tradingsymbol: str) -> Dict[str, float]:
    quote_key = f"{exchange}:{tradingsymbol}"
    payload = kite.quote([quote_key])[quote_key]

    last_price = float(payload.get("last_price") or 0.0)
    bid = 0.0
    ask = 0.0

    depth = payload.get("depth") or {}
    buy_depth = depth.get("buy") or []
    sell_depth = depth.get("sell") or []

    if buy_depth and buy_depth[0].get("price") is not None:
        bid = float(buy_depth[0]["price"])
    if sell_depth and sell_depth[0].get("price") is not None:
        ask = float(sell_depth[0]["price"])

    if bid <= 0 and ask > 0:
        bid = ask
    if ask <= 0 and bid > 0:
        ask = bid
    if bid <= 0 and ask <= 0:
        bid = last_price
        ask = last_price

    return {
        "quote_key": quote_key,
        "last_price": last_price,
        "bid": bid,
        "ask": ask,
        "spread": max(0.0, ask - bid),
    }


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


def build_gtt_match_key_from_plan(plan: Dict) -> Tuple:
    return (
        plan["tradingsymbol"],
        plan["exchange"],
        plan["exit_transaction_type"],
        int(plan["quantity"]),
        plan["product"],
    )


def extract_trigger_id(resp: Any) -> int:
    if isinstance(resp, dict):
        if "trigger_id" in resp:
            return int(resp["trigger_id"])
        data = resp.get("data")
        if isinstance(data, dict) and "trigger_id" in data:
            return int(data["trigger_id"])
    return int(resp)


def determine_effective_multiplier(atr: float, atr_average: float, config: StopLossConfig) -> float:
    if atr_average > 0 and atr >= config.volatility_spike_ratio * atr_average:
        return round(config.base_multiplier * config.volatility_spike_multiplier, 8)
    return config.base_multiplier


def compute_min_distance(last_price: float, atr: float, config: StopLossConfig) -> float:
    return max(last_price * config.min_distance_pct, atr * config.min_distance_atr_multiple)


def compute_spread_threshold(last_price: float, tick_size: float, config: StopLossConfig) -> float:
    return max(
        last_price * config.spread_threshold_pct,
        tick_size * config.spread_threshold_tick_multiple,
    )


def compute_buffer(
    last_price: float,
    tick_size: float,
    spread: float,
    spread_threshold: float,
    config: StopLossConfig,
) -> float:
    base_buffer = max(last_price * config.base_buffer_pct, tick_size)
    if spread > spread_threshold:
        return base_buffer * 2.0
    return base_buffer


def material_change_required(
    old_trigger: Optional[float],
    new_trigger: float,
    last_price: float,
    tick_size: float,
    config: StopLossConfig,
) -> bool:
    if old_trigger is None:
        return True
    threshold = max(
        tick_size * config.material_change_tick_multiple,
        last_price * config.material_change_pct,
    )
    return abs(new_trigger - old_trigger) >= threshold


def calculate_deterministic_stop(
    candles: List[Dict],
    side: str,
    tick_size: float,
    entry_price: float,
    current_price: float,
    bid: float,
    ask: float,
    config: StopLossConfig,
    previous_trigger_price: Optional[float] = None,
) -> Dict:
    if side not in {"LONG", "SHORT"}:
        raise ValueError("side must be LONG or SHORT")

    min_needed = max(config.atr_period + config.atr_average_window, config.swing_lookback + 1)
    if len(candles) < min_needed:
        raise ValueError(f"Need at least {min_needed} candles to compute deterministic stop")

    atr = compute_atr(candles, config.atr_period)
    atr_average = compute_atr_average(candles, config.atr_period, config.atr_average_window)
    multiplier = determine_effective_multiplier(atr, atr_average, config)

    recent = candles[-config.swing_lookback:]
    swing_low = min(float(c["low"]) for c in recent)
    swing_high = max(float(c["high"]) for c in recent)

    spread = max(0.0, ask - bid)
    spread_threshold = compute_spread_threshold(current_price, tick_size, config)
    min_distance = compute_min_distance(current_price, atr, config)
    buffer_value = compute_buffer(current_price, tick_size, spread, spread_threshold, config)

    if side == "LONG":
        initial_stop = entry_price - (atr * multiplier)
        trailing_candidate = max(
            swing_low,
            current_price - (atr * multiplier),
        )
        raw_stop = max(initial_stop, trailing_candidate) if previous_trigger_price is None else max(previous_trigger_price, trailing_candidate)
        validated_stop = min(raw_stop, current_price - min_distance)
        validated_stop = floor_to_tick(validated_stop, tick_size)
        if previous_trigger_price is not None:
            validated_stop = max(validated_stop, floor_to_tick(previous_trigger_price, tick_size))
        if validated_stop >= current_price:
            validated_stop = floor_to_tick(current_price - min_distance, tick_size)

        trigger_price = validated_stop
        limit_price = floor_to_tick(trigger_price - buffer_value, tick_size)
        if limit_price > trigger_price:
            limit_price = trigger_price

    else:
        initial_stop = entry_price + (atr * multiplier)
        trailing_candidate = min(
            swing_high,
            current_price + (atr * multiplier),
        )
        raw_stop = min(initial_stop, trailing_candidate) if previous_trigger_price is None else min(previous_trigger_price, trailing_candidate)
        validated_stop = max(raw_stop, current_price + min_distance)
        validated_stop = ceil_to_tick(validated_stop, tick_size)
        if previous_trigger_price is not None:
            validated_stop = min(validated_stop, ceil_to_tick(previous_trigger_price, tick_size))
        if validated_stop <= current_price:
            validated_stop = ceil_to_tick(current_price + min_distance, tick_size)

        trigger_price = validated_stop
        limit_price = ceil_to_tick(trigger_price + buffer_value, tick_size)
        if limit_price < trigger_price:
            limit_price = trigger_price

    return {
        "entry_price": round(entry_price, 4),
        "current_price": round(current_price, 4),
        "bid": round(bid, 4),
        "ask": round(ask, 4),
        "spread": round(spread, 4),
        "spread_threshold": round(spread_threshold, 4),
        "atr": round(atr, 4),
        "atr_average": round(atr_average, 4),
        "multiplier": round(multiplier, 4),
        "swing_low": round(swing_low, 4),
        "swing_high": round(swing_high, 4),
        "initial_stop": round(initial_stop, 4),
        "trailing_candidate": round(trailing_candidate, 4),
        "previous_trigger_price": round(previous_trigger_price, 4) if previous_trigger_price is not None else None,
        "raw_stop": round(raw_stop, 4),
        "min_distance": round(min_distance, 4),
        "buffer_value": round(buffer_value, 4),
        "trigger_price": round(trigger_price, 8),
        "limit_price": round(limit_price, 8),
    }


def build_stoploss_plans(
    user_id: str,
    contract_type: str = "near",
    symbol: Optional[str] = None,
    config: StopLossConfig = StopLossConfig(),
) -> List[Dict]:
    kite = get_kite_client(user_id)
    positions = find_positions(
        user_id=user_id,
        contract_type=contract_type,
        symbol=symbol,
    )

    if not positions:
        return []

    existing_gtt_map = get_existing_active_gtt_map(kite)
    plans: List[Dict] = []

    for position in positions:
        tradingsymbol = position["tradingsymbol"]
        exchange = position.get("exchange") or "NFO"
        quantity = int(position["quantity"])
        abs_quantity = abs(quantity)
        entry_price = float(position["avg_price"])
        side = "LONG" if quantity > 0 else "SHORT"
        transaction_type = "SELL" if side == "LONG" else "BUY"
        tick_size = float(position.get("tick_size") or 0.05)
        instrument_token = int(position["instrument_token"])

        key = (tradingsymbol, exchange, transaction_type, abs_quantity, "NRML")
        existing_gtt = existing_gtt_map.get(key)
        previous_trigger_price = extract_existing_trigger_price(existing_gtt)
        previous_limit_price = extract_existing_limit_price(existing_gtt)

        candles = get_daily_candles(kite, instrument_token, config)
        quote = get_quote_snapshot(kite, exchange, tradingsymbol)

        stop = calculate_deterministic_stop(
            candles=candles,
            side=side,
            tick_size=tick_size,
            entry_price=entry_price,
            current_price=float(quote["last_price"]),
            bid=float(quote["bid"]),
            ask=float(quote["ask"]),
            config=config,
            previous_trigger_price=previous_trigger_price,
        )

        trigger_price = float(stop["trigger_price"])
        limit_price = float(stop["limit_price"])

        if side == "LONG":
            per_unit_risk = max(0.0, entry_price - trigger_price)
        else:
            per_unit_risk = max(0.0, trigger_price - entry_price)

        total_risk = per_unit_risk * abs_quantity
        update_required = material_change_required(
            old_trigger=previous_trigger_price,
            new_trigger=trigger_price,
            last_price=float(quote["last_price"]),
            tick_size=tick_size,
            config=config,
        )

        stop_hash = build_stop_hash(trigger_price, limit_price, abs_quantity)
        existing_stop_hash = None
        if previous_trigger_price is not None and previous_limit_price is not None:
            existing_stop_hash = build_stop_hash(previous_trigger_price, previous_limit_price, abs_quantity)

        plans.append({
            "user_id": user_id,
            "contract_type": contract_type,
            "symbol_filter": symbol,
            "tradingsymbol": tradingsymbol,
            "position_side": side,
            "exit_transaction_type": transaction_type,
            "quantity": abs_quantity,
            "product": "NRML",
            "exchange": exchange,
            "entry_price": entry_price,
            "current_price": float(quote["last_price"]),
            "per_unit_risk": round(per_unit_risk, 2),
            "total_risk": round(total_risk, 2),
            "trigger_price": trigger_price,
            "limit_price": limit_price,
            "existing_trigger_price": previous_trigger_price,
            "existing_limit_price": previous_limit_price,
            "existing_gtt_id": existing_gtt.get("id") if existing_gtt else None,
            "update_required": update_required,
            "stop_hash": stop_hash,
            "existing_stop_hash": existing_stop_hash,
            "details": stop,
        })

    return plans


def place_stoploss_gtt(
    user_id: str,
    contract_type: str = "near",
    symbol: Optional[str] = None,
    dry_run: bool = True,
    config: StopLossConfig = StopLossConfig(),
) -> Dict:
    kite = get_kite_client(user_id)
    plans = build_stoploss_plans(
        user_id=user_id,
        contract_type=contract_type,
        symbol=symbol,
        config=config,
    )

    if not plans:
        return {
            "mode": "DRY_RUN" if dry_run else "LIVE",
            "plans": [] if dry_run else None,
            "results": [] if not dry_run else None,
            "message": f"No {contract_type}-month futures position found.",
        }

    if dry_run:
        return {
            "mode": "DRY_RUN",
            "plans": plans,
        }

    existing_gtt_map = get_existing_active_gtt_map(kite)
    results = []

    for plan in plans:
        key = build_gtt_match_key_from_plan(plan)
        existing_gtt = existing_gtt_map.get(key)

        if existing_gtt and not plan["update_required"]:
            results.append({
                "trigger_id": int(existing_gtt["id"]),
                "action": "UNCHANGED",
                "plan": plan,
            })
            continue

        kwargs = dict(
            trigger_type=kite.GTT_TYPE_SINGLE,
            tradingsymbol=plan["tradingsymbol"],
            exchange=plan["exchange"],
            trigger_values=[plan["trigger_price"]],
            last_price=plan["current_price"],
            orders=[
                {
                    "exchange": plan["exchange"],
                    "tradingsymbol": plan["tradingsymbol"],
                    "transaction_type": plan["exit_transaction_type"],
                    "quantity": plan["quantity"],
                    "order_type": "LIMIT",
                    "product": plan["product"],
                    "price": plan["limit_price"],
                }
            ],
        )

        if existing_gtt:
            resp = call_with_retries(
                kite.modify_gtt,
                trigger_id=int(existing_gtt["id"]),
                retries=config.retries,
                base_delay_sec=config.retry_base_delay_sec,
                **kwargs,
            )
            action = "MODIFIED"
        else:
            resp = call_with_retries(
                kite.place_gtt,
                retries=config.retries,
                base_delay_sec=config.retry_base_delay_sec,
                **kwargs,
            )
            action = "PLACED"

        results.append({
            "trigger_id": extract_trigger_id(resp),
            "action": action,
            "plan": plan,
        })

    return {
        "mode": "LIVE",
        "results": results,
    }


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
    print("  python kite_stoploss_service_production.py <USER_ID> [contract_type] [symbol] [dry_run]")
    print("")
    print("Examples:")
    print("  python kite_stoploss_service_production.py XJ1877")
    print("  python kite_stoploss_service_production.py XJ1877 near HDFCBANK")
    print('  python kite_stoploss_service_production.py XJ1877 near "M&M" true')
    print("  python kite_stoploss_service_production.py XJ1877 near HDFCBANK false")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print_help()
        sys.exit(1)

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
        result = place_stoploss_gtt(
            user_id=user_id,
            contract_type=contract_type,
            symbol=symbol,
            dry_run=dry_run,
            config=StopLossConfig(),
        )

        if result["mode"] == "DRY_RUN":
            plans = result["plans"]
            if not plans:
                print(result.get("message", f"No {contract_type}-month futures position found."))
                sys.exit(0)

            rows = []
            for plan in plans:
                rows.append([
                    plan["tradingsymbol"],
                    plan["contract_type"],
                    plan["position_side"],
                    plan["exit_transaction_type"],
                    str(plan["quantity"]),
                    f"{plan['entry_price']:.2f}",
                    f"{plan['current_price']:.2f}",
                    f"{plan['trigger_price']:.2f}",
                    f"{plan['limit_price']:.2f}",
                    f"{plan['details']['atr']:.2f}",
                    f"{plan['details']['atr_average']:.2f}",
                    f"{plan['details']['multiplier']:.2f}",
                    f"{plan['details']['raw_stop']:.2f}",
                    f"{plan['per_unit_risk']:.2f}",
                    f"{plan['total_risk']:.2f}",
                    "YES" if plan["update_required"] else "NO",
                ])

            print("DRY RUN")
            print_table(
                headers=[
                    "Tradingsymbol",
                    "Month",
                    "Side",
                    "Exit",
                    "Qty",
                    "Entry Price",
                    "Current Price",
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
        else:
            results = result["results"]
            if not results:
                print(result.get("message", f"No {contract_type}-month futures position found."))
                sys.exit(0)

            rows = []
            for item in results:
                plan = item["plan"]
                rows.append([
                    item["action"],
                    str(item["trigger_id"]),
                    plan["tradingsymbol"],
                    plan["contract_type"],
                    plan["position_side"],
                    str(plan["quantity"]),
                    f"{plan['trigger_price']:.2f}",
                    f"{plan['limit_price']:.2f}",
                ])

            print("LIVE GTT UPSERT")
            print_table(
                headers=[
                    "Action",
                    "Trigger ID",
                    "Tradingsymbol",
                    "Month",
                    "Side",
                    "Qty",
                    "Trigger",
                    "Limit",
                ],
                rows=rows,
            )

    except Exception as exc:
        print(f"ERROR: {exc}")
        sys.exit(1)
