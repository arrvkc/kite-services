import sys
from datetime import datetime
from typing import List, Dict, Optional

from kiteconnect import KiteConnect

from kite_credentials_service import get_kite_credentials


def get_grouped_futures_map(kite):
    instruments = kite.instruments("NFO")
    today = datetime.now().date()

    grouped = {}
    for i in instruments:
        if i.get("instrument_type") != "FUT":
            continue

        expiry = i.get("expiry")
        if not expiry or expiry < today:
            continue

        name = i.get("name")
        if not name:
            continue

        grouped.setdefault(name, []).append(i)

    for name in grouped:
        grouped[name] = sorted(grouped[name], key=lambda x: x["expiry"])

    return grouped


def get_all_open_futures_positions(user_id: str) -> List[Dict]:
    """
    Fetch all open futures positions for the user, across expiries.
    """
    api_key, access_token = get_kite_credentials(user_id)

    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)

    positions = kite.positions()["net"]

    results = []
    for p in positions:
        ts = p.get("tradingsymbol", "")
        qty = p.get("quantity", 0)

        if not ts.endswith("FUT"):
            continue

        if qty == 0:
            continue

        results.append({
            "tradingsymbol": ts,
            "quantity": qty,
            "avg_price": p.get("average_price"),
            "pnl": p.get("pnl"),
        })

    return results


def build_rollover_orders(user_id: str, symbol: Optional[str] = None) -> List[Dict]:
    """
    Build rollover orders only from market near-month to market next-month.

    If symbol is provided, only build for that underlying, e.g. HDFCBANK.
    """
    api_key, access_token = get_kite_credentials(user_id)

    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)

    grouped_map = get_grouped_futures_map(kite)
    positions = get_all_open_futures_positions(user_id)

    symbol_filter = symbol.upper() if symbol else None
    position_map = {p["tradingsymbol"]: p for p in positions}

    orders = []

    for underlying, contracts in grouped_map.items():
        if symbol_filter and underlying.upper() != symbol_filter:
            continue

        # Need at least near and next
        if len(contracts) < 2:
            continue

        near_contract = contracts[0]["tradingsymbol"]
        next_contract = contracts[1]["tradingsymbol"]

        # Only roll if the user actually holds the market near contract
        p = position_map.get(near_contract)
        if not p:
            continue

        qty = p["quantity"]

        if qty > 0:
            # Long near -> sell near, buy next
            orders.append({
                "tradingsymbol": near_contract,
                "transaction_type": "SELL",
                "quantity": abs(qty)
            })
            orders.append({
                "tradingsymbol": next_contract,
                "transaction_type": "BUY",
                "quantity": abs(qty)
            })
        elif qty < 0:
            # Short near -> buy near, sell next
            orders.append({
                "tradingsymbol": near_contract,
                "transaction_type": "BUY",
                "quantity": abs(qty)
            })
            orders.append({
                "tradingsymbol": next_contract,
                "transaction_type": "SELL",
                "quantity": abs(qty)
            })

    return orders


def execute_rollover(user_id: str, dry_run: bool = True, symbol: Optional[str] = None):
    """
    Execute rollover orders. Dry run by default.
    """
    api_key, access_token = get_kite_credentials(user_id)

    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)

    orders = build_rollover_orders(user_id, symbol=symbol)

    if not orders:
        print("No rollover orders generated.")
        return

    print(f"Total Orders: {len(orders)}")

    for o in orders:
        if dry_run:
            print("DRY RUN:", o)
        else:
            kite.place_order(
                variety="regular",
                exchange="NFO",
                tradingsymbol=o["tradingsymbol"],
                transaction_type=o["transaction_type"],
                quantity=o["quantity"],
                product="NRML",
                order_type="MARKET"
            )
            print("Executed:", o)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage:")
        print("python kite_rollover_service.py <USER_ID> [dry_run] [symbol]")
        print("dry_run: true/false (default true)")
        print("symbol: underlying only, e.g. HDFCBANK")
        print("Examples:")
        print("  python kite_rollover_service.py XJ1877")
        print("  python kite_rollover_service.py XJ1877 true HDFCBANK")
        print("  python kite_rollover_service.py OMK569 false 'M&M'")
        print("Note: quote symbols containing special characters like &, e.g. 'M&M'")
        sys.exit(1)

    user_id = sys.argv[1]

    dry_run = True
    symbol = None

    if len(sys.argv) >= 3:
        arg2 = sys.argv[2].lower()
        if arg2 in ("true", "false"):
            dry_run = arg2 == "true"
            if len(sys.argv) >= 4:
                symbol = sys.argv[3].upper()
        else:
            symbol = sys.argv[2].upper()

    execute_rollover(user_id, dry_run=dry_run, symbol=symbol)
