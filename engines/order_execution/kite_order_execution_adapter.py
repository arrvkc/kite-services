import logging
import hashlib
import time
import sys
import requests
from services.kite_credentials_service import get_kite_credentials
from kiteconnect import KiteConnect

# Set up logging configuration
logging.basicConfig(level=logging.INFO)

class KiteOrderExecutionAdapter:
    def __init__(self, user_id, dry_run=False):
        """Initialize the KiteOrderExecutionAdapter with credentials."""
        self.dry_run = dry_run
        try:
            # Fetch credentials using the user_id from the credentials service
            self.api_key, self.access_token = get_kite_credentials(user_id)

            # Initialize KiteConnect using the fetched credentials
            self.kite = KiteConnect(api_key=self.api_key)
            self.kite.set_access_token(self.access_token)
        except Exception as e:
            logging.error(f"Error fetching credentials: {e}")
            sys.exit(1)

    def create_order_hash(self, trigger_price, limit_price, quantity):
        """Create a unique hash for an order based on its parameters."""
        order_data = f"{trigger_price}{limit_price}{quantity}"
        return hashlib.sha256(order_data.encode()).hexdigest()

    def place_order(self, position_type, trigger_price, limit_price, quantity, tradingsymbol, exchange, order_type):
        """Place an order (GTT, LIMIT, or MARKET) using KiteConnect."""
        if self.dry_run:
            logging.info(
                f"Dry run: Simulated placing {order_type} order for {tradingsymbol} at trigger price {trigger_price}, limit price {limit_price} for {quantity} units.")
            return "DRY_RUN_ORDER_ID"

        try:
            order_hash = self.create_order_hash(trigger_price, limit_price, quantity)
            logging.info(f"Placing {order_type} order with hash: {order_hash}")

            # Define the order parameters
            order_params = {
                "tradingsymbol": tradingsymbol,
                "exchange": exchange,
                "transaction_type": "BUY" if position_type == "BUY" else "SELL",
                "quantity": quantity,
                "price": limit_price,
                "trigger_price": trigger_price,
                "validity": "GTT" if order_type == "GTT" else "DAY",  # For GTT, the validity is different
                "order_type": "LIMIT" if order_type == "LIMIT" else "MARKET",  # Handle market orders
            }

            if order_type == "GTT":
                # GTT order API endpoint (manual HTTP request, as KiteConnect does not support this directly)
                gtt_url = "https://api.kite.trade/gtt/orders"
                headers = {
                    "Authorization": f"Bearer {self.access_token}",  # Ensure this header is set correctly
                    "Content-Type": "application/json"  # Use application/json instead of x-www-form-urlencoded
                }
                logging.info(f"Headers: {headers}")  # Debug: Log the headers
                response = requests.post(gtt_url, json=order_params, headers=headers)  # Use json parameter instead of data
                if response.status_code == 200:
                    order_data = response.json()
                    logging.info(f"GTT Order placed successfully with Order ID: {order_data['order_id']}")
                    return order_data["order_id"]
                else:
                    logging.error(f"Failed to place GTT order: {response.text}")
                    raise Exception(f"Failed to place GTT order: {response.text}")
            elif order_type == "LIMIT" or order_type == "MARKET":
                # Place LIMIT or MARKET order via KiteConnect API
                order_id = self.kite.place_order(**order_params)
                logging.info(f"Order placed successfully with Order ID: {order_id}")
                return order_id
            else:
                logging.error(f"Unsupported order type: {order_type}")
                raise ValueError(f"Unsupported order type: {order_type}")

        except Exception as e:
            logging.error(f"Error placing {order_type} order: {e}")
            raise

    def retry_order(self, position_type, trigger_price, limit_price, quantity, tradingsymbol, exchange, order_type,
                    retries=3, base_delay=1):
        """Retry order placement with exponential backoff."""
        for attempt in range(retries):
            try:
                logging.info(f"Attempt {attempt + 1}: Trying to place {order_type} order...")
                return self.place_order(position_type, trigger_price, limit_price, quantity, tradingsymbol, exchange,
                                        order_type)
            except Exception as e:
                logging.error(f"Error placing {order_type} order: {e}")
                time.sleep(base_delay * (2 ** attempt))  # Exponential backoff
        raise Exception(f"Failed to place {order_type} order after multiple retries")


# Main function to accept command-line arguments
def main():
    if len(sys.argv) not in [8, 9]:
        print(
            "Usage: python kite_order_execution_adapter.py <user_id> <position_type> <tradingsymbol> <exchange> <trigger_price> <limit_price> <quantity> <order_type> [--dry-run]")
        sys.exit(1)

    # Get the arguments from the command line
    user_id = sys.argv[1]
    position_type = sys.argv[2].upper()  # Position type (BUY/SELL)
    tradingsymbol = sys.argv[3]
    exchange = sys.argv[4]
    trigger_price = float(sys.argv[5])
    limit_price = float(sys.argv[6])
    quantity = int(sys.argv[7])
    order_type = sys.argv[8].upper()  # GTT, LIMIT, or MARKET

    # Check for dry-run flag
    dry_run = '--dry-run' in sys.argv

    # Create the KiteOrderExecutionAdapter instance
    kite_adapter = KiteOrderExecutionAdapter(user_id, dry_run)

    # Place the order using the provided arguments
    kite_adapter.retry_order(position_type=position_type, trigger_price=trigger_price, limit_price=limit_price,
                             quantity=quantity, tradingsymbol=tradingsymbol, exchange=exchange, order_type=order_type)


if __name__ == "__main__":
    main()