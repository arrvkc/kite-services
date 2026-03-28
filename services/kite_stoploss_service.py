import sys
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple

from kiteconnect import KiteConnect

from kite_credentials_service import get_kite_credentials
from kite_market_data_service import get_all_futures_positions


VALID_CONTRACT_TYPES = {"near", "next", "far"}


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
    """
    Build market expiry ladder from instrument master:
    underlying -> sorted live futures contracts
    """
    today = datetime.now().date()
    instruments = get_nfo_futures_instruments(kite)

    grouped = {}

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

    # User's actual open futures positions
    positions = get_all_futures_positions(
        user_id=user_id,
        exclude_zero_qty=True,
    )

    if symbol:
        symbol_upper = symbol.upper()
        positions = [p for p in positions if p["underlying"].upper() == symbol_upper]

    if not positions:
        return []

    # Market contract ladder by underlying
    ladder_map = get_market_expiry_ladder(kite, symbol=symbol)

    expiry_index_map = {
        "near": 0,
        "next": 1,
        "far": 2,
    }
    idx = expiry_index_map[contract_type]

    selected_positions = []

    # Group user's positions by underlying
    positions_by_underlying = {}
    for p in positions:
        positions_by_underlying.setdefault(p["underlying"], []).append(p)

    for underlying, held_positions in positions_by_underlying.items():
        ladder = ladder_map.get(underlying, [])
        if idx >= len(ladder):
            continue

        selected_contract = ladder[idx]["tradingsymbol"]

        matched = [p for p in held_positions if p["tradingsymbol"] == selected_contract]
        selected_positions.extend(matched)

    if not selected_positions:
        return []

    return selected_positions


def get_instrument_for_tradingsymbol(kite: KiteConnect, tradingsymbol: str) -> Dict:
    instruments = get_nfo_futures_instruments(kite)
    for inst in instruments:
        if inst.get("tradingsymbol") == tradingsymbol:
            return inst
    raise ValueError(f"Instrument metadata not found for {tradingsymbol}")


def true_range(high: float, low: float, prev_close: float) -> float:
    return max(
        high - low,
        abs(high - prev_close),
        abs(low - prev_close),
    )


def compute_atr(candles: List[Dict], period: int = 14) -> float:
    if len(candles) < period + 1:
        raise ValueError(f"Need at least {period + 1} candles to compute ATR({period})")

    trs = []
    for i in range(1, len(candles)):
        h = candles[i]["high"]
        l = candles[i]["low"]
        prev_close = candles[i - 1]["close"]
        trs.append(true_range(h, l, prev_close))

    recent_trs = trs[-period:]
    return sum(recent_trs) / len(recent_trs)


def round_to_tick(price: float, tick_size: float) -> float:
    if tick_size <= 0:
        return round(price, 2)
    return round(round(price / tick_size) * tick_size, 8)


def candles_to_dicts(raw_candles: List[Dict]) -> List[Dict]:
    out = []
    for c in raw_candles:
        out.append(
            {
                "date": c["date"],
                "open": float(c["open"]),
                "high": float(c["high"]),
                "low": float(c["low"]),
                "close": float(c["close"]),
                "volume": c.get("volume"),
                "oi": c.get("oi"),
            }
        )
    return out


def get_daily_candles(
    kite: KiteConnect,
    instrument_token: int,
    days: int = 90,
) -> List[Dict]:
    to_dt = datetime.now()
    from_dt = to_dt - timedelta(days=days)
    raw = kite.historical_data(
        instrument_token=instrument_token,
        from_date=from_dt,
        to_date=to_dt,
        interval="day",
        continuous=False,
        oi=True,
    )
    candles = candles_to_dicts(raw)
    if len(candles) < 20:
        raise ValueError("Not enough daily candles returned for stop calculation")
    return candles


def calculate_noise_filtered_stop(
    candles: List[Dict],
    side: str,
    tick_size: float,
    entry_price: float,
    atr_period: int = 14,
    atr_multiple: float = 2.5,
    swing_lookback: int = 5,
    trigger_buffer_pct: float = 0.0015,
    limit_buffer_pct: float = 0.0025,
) -> Dict:
    if side not in {"LONG", "SHORT"}:
        raise ValueError("side must be LONG or SHORT")

    if len(candles) < max(atr_period + 1, swing_lookback + 1):
        raise ValueError("Not enough candles for stop calculation")

    atr = compute_atr(candles, period=atr_period)
    last_close = candles[-1]["close"]

    recent = candles[-swing_lookback:]
    swing_low = min(c["low"] for c in recent)
    swing_high = max(c["high"] for c in recent)

    if side == "LONG":
        market_stop = min(swing_low, last_close - (atr_multiple * atr))
        entry_stop = entry_price - (atr_multiple * atr)

        raw_stop = max(market_stop, entry_stop)

        trigger_price = raw_stop * (1 - trigger_buffer_pct)
        limit_price = raw_stop * (1 - limit_buffer_pct)

        trigger_price = round_to_tick(trigger_price, tick_size)
        limit_price = round_to_tick(limit_price, tick_size)

        if limit_price > trigger_price:
            limit_price = trigger_price

    else:
        market_stop = max(swing_high, last_close + (atr_multiple * atr))
        entry_stop = entry_price + (atr_multiple * atr)

        raw_stop = min(market_stop, entry_stop)

        trigger_price = raw_stop * (1 + trigger_buffer_pct)
        limit_price = raw_stop * (1 + limit_buffer_pct)

        trigger_price = round_to_tick(trigger_price, tick_size)
        limit_price = round_to_tick(limit_price, tick_size)

        if limit_price < trigger_price:
            limit_price = trigger_price

    return {
        "last_close": round(last_close, 4),
        "entry_price": round(entry_price, 4),
        "atr": round(atr, 4),
        "swing_low": round(swing_low, 4),
        "swing_high": round(swing_high, 4),
        "market_stop": round(market_stop, 4),
        "entry_stop": round(entry_stop, 4),
        "raw_stop": round(raw_stop, 4),
        "trigger_price": trigger_price,
        "limit_price": limit_price,
    }


def build_stoploss_plans(
    user_id: str,
    contract_type: str = "near",
    symbol: Optional[str] = None,
    atr_period: int = 14,
    atr_multiple: float = 2.5,
    swing_lookback: int = 5,
) -> List[Dict]:
    kite = get_kite_client(user_id)

    positions = find_positions(
        user_id=user_id,
        contract_type=contract_type,
        symbol=symbol,
    )

    if not positions:
        return []

    plans = []

    for position in positions:
        tradingsymbol = position["tradingsymbol"]
        quantity = int(position["quantity"])
        entry_price = float(position["avg_price"])
        side = "LONG" if quantity > 0 else "SHORT"

        instrument_token = int(position["instrument_token"])
        tick_size = float(position.get("tick_size", 0.05))

        candles = get_daily_candles(kite, instrument_token, days=90)
        stop = calculate_noise_filtered_stop(
            candles=candles,
            side=side,
            tick_size=tick_size,
            entry_price=entry_price,
            atr_period=atr_period,
            atr_multiple=atr_multiple,
            swing_lookback=swing_lookback,
        )

        transaction_type = "SELL" if side == "LONG" else "BUY"

        quote_key = f"NFO:{tradingsymbol}"
        ltp_data = kite.ltp([quote_key])
        last_price = float(ltp_data[quote_key]["last_price"])
        trigger_price = stop["trigger_price"]

        if side == "LONG":
            per_unit_loss = entry_price - trigger_price
        else:
            per_unit_loss = trigger_price - entry_price

        total_loss = per_unit_loss * abs(quantity)

        plans.append({
            "user_id": user_id,
            "contract_type": contract_type,
            "symbol_filter": symbol,
            "tradingsymbol": tradingsymbol,
            "position_side": side,
            "exit_transaction_type": transaction_type,
            "quantity": abs(quantity),
            "product": "NRML",
            "exchange": "NFO",
            "entry_price": entry_price,
            "per_unit_loss": round(per_unit_loss, 2),
            "total_loss": round(total_loss, 2),
            "last_price": last_price,
            "trigger_price": stop["trigger_price"],
            "limit_price": stop["limit_price"],
            "details": stop,
        })

    return plans


def build_gtt_match_key_from_plan(plan: Dict) -> Tuple:
    return (
        plan["tradingsymbol"],
        plan["exchange"],
        plan["exit_transaction_type"],
        int(plan["quantity"]),
        plan["product"],
    )


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
    existing = kite.get_gtts()
    gtt_map = {}

    for gtt in existing:
        key = build_gtt_match_key_from_existing(gtt)
        if key is None:
            continue
        gtt_map[key] = gtt

    return gtt_map


def extract_trigger_id(resp) -> int:
    if isinstance(resp, dict):
        if "trigger_id" in resp:
            return int(resp["trigger_id"])
        data = resp.get("data")
        if isinstance(data, dict) and "trigger_id" in data:
            return int(data["trigger_id"])
    return int(resp)


def place_stoploss_gtt(
    user_id: str,
    contract_type: str = "near",
    symbol: Optional[str] = None,
    dry_run: bool = True,
    atr_period: int = 14,
    atr_multiple: float = 2.5,
    swing_lookback: int = 5,
) -> Dict:
    kite = get_kite_client(user_id)

    plans = build_stoploss_plans(
        user_id=user_id,
        contract_type=contract_type,
        symbol=symbol,
        atr_period=atr_period,
        atr_multiple=atr_multiple,
        swing_lookback=swing_lookback,
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

        if existing_gtt:
            resp = kite.modify_gtt(
                trigger_id=existing_gtt["id"],
                trigger_type=kite.GTT_TYPE_SINGLE,
                tradingsymbol=plan["tradingsymbol"],
                exchange=plan["exchange"],
                trigger_values=[plan["trigger_price"]],
                last_price=plan["last_price"],
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
            trigger_id = extract_trigger_id(resp)
            action = "MODIFIED"
        else:
            resp = kite.place_gtt(
                trigger_type=kite.GTT_TYPE_SINGLE,
                tradingsymbol=plan["tradingsymbol"],
                exchange=plan["exchange"],
                trigger_values=[plan["trigger_price"]],
                last_price=plan["last_price"],
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
            trigger_id = extract_trigger_id(resp)
            action = "PLACED"

        results.append({
            "trigger_id": trigger_id,
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

    def fmt_row(row_vals: List[str]) -> str:
        return " | ".join(str(v).ljust(widths[i]) for i, v in enumerate(row_vals))

    separator = "-+-".join("-" * w for w in widths)

    print(fmt_row(headers))
    print(separator)
    for row in rows:
        print(fmt_row(row))


def print_help() -> None:
    print("Usage:")
    print("  python kite_stoploss_service.py <USER_ID> [contract_type] [symbol] [dry_run]")
    print("")
    print("Arguments:")
    print("  <USER_ID>       Zerodha user id, e.g. XJ1877")
    print("  [contract_type] near | next | far   (default: near)")
    print("  [symbol]        underlying only, e.g. HDFCBANK or 'M&M'")
    print("  [dry_run]       true | false        (default: true)")
    print("")
    print("Examples:")
    print("  python kite_stoploss_service.py XJ1877")
    print("  python kite_stoploss_service.py XJ1877 near HDFCBANK")
    print("  python kite_stoploss_service.py XJ1877 next HDFCBANK true")
    print('  python kite_stoploss_service.py OMK569 near "M&M" true')
    print("  python kite_stoploss_service.py XJ1877 near HDFCBANK false")
    print("")
    print("Notes:")
    print("  - Without symbol, it processes all matching positions for the selected contract type.")
    print("  - dry_run=true only shows the proposed stop and GTT payload.")
    print('  - Use quotes for symbols with special characters, e.g. "M&M".')


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
                    f"{plan['last_price']:.2f}",
                    f"{plan['trigger_price']:.2f}",
                    f"{plan['limit_price']:.2f}",
                    f"{plan['details']['atr']:.2f}",
                    f"{plan['details']['raw_stop']:.2f}",
                    f"{plan['per_unit_loss']:.2f}",
                    f"{plan['total_loss']:.2f}",
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
                    "Last Price",
                    "Trigger",
                    "Limit",
                    "ATR",
                    "Raw Stop",
                    "Loss/Unit",
                    "Total Loss",
                ],
                rows=rows,
            )
        else:
            results = result["results"]
            if not results:
                print(result.get("message", f"No {contract_type}-month futures position found."))
                sys.exit(0)
            rows = []
            for item in result["results"]:
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

    except Exception as e:
        print(f"ERROR: {e}")
        sys.exit(1)
