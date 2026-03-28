import sys
from datetime import datetime
from typing import List, Dict

from kiteconnect import KiteConnect
from kite_credentials_service import get_kite_credentials


# ===============================
# CORE FUNCTION
# ===============================
def get_futures_positions(
    user_id: str,
    contract_type: str = "near",   # near / next / far
    exclude_zero_qty: bool = True
) -> List[Dict]:
    """
    Fetch futures positions (near / next / far) for a given user.
    """

    api_key, access_token = get_kite_credentials(user_id)

    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)

    positions = kite.positions()["net"]
    instruments = kite.instruments("NFO")

    instrument_map = {
        i["tradingsymbol"]: {
            "expiry": i["expiry"],
            "underlying": i["name"],
            "instrument_token": i["instrument_token"],
            "tick_size": i.get("tick_size", 0.05),
        }
        for i in instruments
        if i["instrument_type"] == "FUT"
    }

    today = datetime.now().date()
    results = []

    # Collect valid futures positions
    for p in positions:
        ts = p.get("tradingsymbol", "")

        if not ts.endswith("FUT"):
            continue

        if exclude_zero_qty and p.get("quantity", 0) == 0:
            continue

        if ts not in instrument_map:
            continue

        expiry = instrument_map[ts]

        if expiry < today:
            continue

        results.append({
            "tradingsymbol": ts,
            "quantity": p.get("quantity"),
            "avg_price": p.get("average_price"),
            "pnl": p.get("pnl"),
            "expiry": expiry,
        })

    if not results:
        return []

    # Get unique sorted expiries
    expiries = sorted(list(set(r["expiry"] for r in results)))

    expiry_map = {
        "near": 0,
        "next": 1,
        "far": 2
    }

    if contract_type not in expiry_map:
        raise Exception("contract_type must be one of: near, next, far")

    idx = expiry_map[contract_type]

    if idx >= len(expiries):
        return []

    selected_expiry = expiries[idx]

    # Filter based on selected expiry
    results = [r for r in results if r["expiry"] == selected_expiry]

    # Convert expiry to string for output
    for r in results:
        r["expiry"] = str(r["expiry"])

    results.sort(key=lambda x: x["tradingsymbol"])

    return results


def get_all_futures_positions(user_id: str, exclude_zero_qty: bool = True) -> List[Dict]:
    """
    Fetch all open futures positions across expiries for a given user.
    Returns positions enriched with actual expiry from instrument master.
    """

    api_key, access_token = get_kite_credentials(user_id)

    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)

    positions = kite.positions()["net"]
    instruments = kite.instruments("NFO")

    instrument_map = {
        i["tradingsymbol"]: {
            "expiry": i["expiry"],
            "underlying": i["name"],
            "instrument_token": i["instrument_token"],
            "tick_size": i.get("tick_size", 0.05),
        }
        for i in instruments
        if i["instrument_type"] == "FUT"
    }

    today = datetime.now().date()
    results = []

    for p in positions:
        ts = p.get("tradingsymbol", "")

        if not ts.endswith("FUT"):
            continue

        qty = int(p.get("quantity", 0))
        if exclude_zero_qty and qty == 0:
            continue

        inst = instrument_map.get(ts)
        if not inst:
            continue

        expiry = inst["expiry"]
        if expiry < today:
            continue

        results.append({
            "tradingsymbol": ts,
            "quantity": qty,
            "avg_price": p.get("average_price"),
            "pnl": p.get("pnl"),
            "expiry": expiry,

            
            "underlying": inst["underlying"],
            "instrument_token": inst["instrument_token"],
            "tick_size": inst["tick_size"],
        })

    results.sort(key=lambda x: (x["expiry"], x["tradingsymbol"]))
    return results


# ===============================
# TEST ENTRY POINT
# ===============================
if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage:")
        print("python kite_market_data_service.py futures_positions <USER_ID> [contract_type] [exclude_zero_qty]")
        print("")
        print("contract_type: near / next / far (default: near)")
        print("exclude_zero_qty: true / false (default: true)")
        sys.exit(1)

    func = sys.argv[1]
    user_id = sys.argv[2]

    contract_type = "near"
    exclude_zero_qty = True

    if len(sys.argv) >= 4:
        contract_type = sys.argv[3].lower()

    if len(sys.argv) == 5:
        exclude_zero_qty = sys.argv[4].lower() == "true"

    try:
        if func == "futures_positions":
            data = get_futures_positions(user_id, contract_type, exclude_zero_qty)

            print("✅ Success")
            if not data:
                print(f"No {contract_type}-month futures positions found.")
            else:
                for d in data:
                    print(d)

        else:
            print(f"❌ Unknown function: {func}")
            sys.exit(1)

    except Exception as e:
        print("❌ Error:", str(e))
        sys.exit(1)
