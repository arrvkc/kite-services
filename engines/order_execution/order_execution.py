import sys
from kiteconnect import KiteConnect
from typing import Optional, Dict, List
from services.kite_credentials_service import get_kite_credentials
from services.kite_market_data_service import get_all_futures_positions


def get_kite_client(user_id: str) -> KiteConnect:
    # Assuming get_kite_credentials() fetches user credentials for Kite API
    api_key, access_token = get_kite_credentials(user_id)
    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    return kite


def find_positions(user_id: str, symbol: Optional[str] = None) -> List[Dict]:
    # This function finds positions based on contract type and symbol
    positions = get_all_futures_positions(user_id=user_id, exclude_zero_qty=True)

    # Filter by symbol if specified
    if symbol:
        symbol_upper = symbol.upper()
        positions = [p for p in positions if p["underlying"].upper() == symbol_upper]

    return positions


def execute_order(kite: KiteConnect, plans: List[Dict]) -> Dict:
    # This function is responsible for executing the orders (e.g., placing LIMIT, MARKET, GTT, etc.)
    results = []

    for plan in plans:
        # Create the order parameters (customizable for any order type)
        order_params = {
            "tradingsymbol": plan["tradingsymbol"],
            "exchange": plan["exchange"],
            "transaction_type": plan["side"],  # Buy or Sell based on position
            "quantity": plan["quantity"],
            "order_type": plan["order_type"],  # Order type (LIMIT, MARKET, etc.)
            "price": plan["limit_price"],  # Price for limit orders
            "product": "NRML",  # Product type (you can customize this if needed)
        }

        if plan["order_type"] == "GTT":
            # Place a GTT order using Kite Connect's `place_gtt_order` function
            gtt_params = {
                "tradingsymbol": plan["tradingsymbol"],
                "exchange": plan["exchange"],
                "trigger": plan["trigger_price"],  # Trigger price for GTT
                "order_type": "LIMIT",  # You can set other order types as needed
                "quantity": plan["quantity"],
                "price": plan["limit_price"],  # Limit price for GTT
                "validity": "DAY",  # GTT validity, can be "DAY", "IOC", etc.
            }
            # Place the GTT order
            order = kite.place_gtt_order(**gtt_params)
        elif plan["order_type"] == "LIMIT":
            # Place LIMIT order
            order = kite.place_order(**order_params)
        elif plan["order_type"] == "MARKET":
            # Place MARKET order
            order_params.pop("price", None)  # No price needed for Market orders
            order = kite.place_order(**order_params)
        else:
            order = kite.place_order(**order_params)  # For any other order types

        results.append({
            "order_id": order.get("order_id"),
            "status": "PLACED"
        })

    return {"status": "success", "results": results}


def place_order(
        user_id: str,
        contract_type: str,
        symbol: Optional[str] = None,
        dry_run: bool = True,
        order_type: str = "LIMIT"  # Generic order type (LIMIT, MARKET, GTT)
) -> Dict:
    kite = get_kite_client(user_id)  # Get Kite client instance
    positions = find_positions(user_id=user_id, symbol=symbol)  # Get positions

    if not positions:
        return {"message": f"No {contract_type}-month futures position found."}

    plans = []
    for position in positions:
        quote = get_quote_snapshot(kite, position['exchange'], position['tradingsymbol'])

        side = "LONG" if position['quantity'] > 0 else "SHORT"
        entry_price = float(position['avg_price'])
        current_price = float(quote['last_price'])
        bid = float(quote['bid'])
        ask = float(quote['ask'])
        tick_size = float(position.get('tick_size', 0.05))

        # Example values for testing, replace with actual calculated values
        trigger_price = entry_price - 10  # Example value
        limit_price = entry_price - 5  # Example value

        # Create the generic order plan with calculated trigger and limit prices
        plans.append({
            "user_id": user_id,
            "tradingsymbol": position["tradingsymbol"],
            "side": side,
            "quantity": abs(int(position["quantity"])),
            "trigger_price": trigger_price,
            "limit_price": limit_price,
            "order_type": order_type,  # Pass the order type
            "details": {"trigger_price": trigger_price, "limit_price": limit_price},  # Add any other details
        })

    if dry_run:
        return {"mode": "DRY_RUN", "plans": plans}

    # Execute the order based on the generated plans
    return execute_order(kite, plans)


if __name__ == '__main__':
    # Fetch the arguments passed from the bash script
    user_id = sys.argv[1]
    contract_type = sys.argv[2]
    symbol = sys.argv[3]
    dry_run = sys.argv[4].lower() == "true"  # Convert to boolean
    order_type = sys.argv[5]

    # Call the place_order function
    place_order(user_id=user_id, contract_type=contract_type, symbol=symbol, dry_run=dry_run, order_type=order_type)