import argparse
import csv
import json
import sys
from pathlib import Path

from kiteconnect import KiteConnect
from kite_credentials_service import get_kite_credentials


def save_csv(trades, filepath: str) -> None:
    if not trades:
        Path(filepath).write_text("", encoding="utf-8")
        return

    fieldnames = []
    seen = set()
    for row in trades:
        for key in row.keys():
            if key not in seen:
                seen.add(key)
                fieldnames.append(key)

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(trades)


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify Zerodha Kite /trades access")
    parser.add_argument("user_id", help="Zerodha user ID")
    parser.add_argument("--host", default="root@eajee.in", help="SSH host for credential lookup")
    parser.add_argument("--db-container", default="postgres", help="Postgres Docker container name")
    parser.add_argument("--db-user", default="atms", help="Postgres DB user")
    parser.add_argument("--db-name", default="atms", help="Postgres DB name")
    parser.add_argument("--print-first", action="store_true", help="Print the first returned trade")
    parser.add_argument("--save-json", help="Save raw trades response to JSON file")
    parser.add_argument("--save-csv", help="Save raw trades response to CSV file")
    args = parser.parse_args()

    try:
        api_key, access_token = get_kite_credentials(
            user_id=args.user_id,
            host=args.host,
            db_container=args.db_container,
            db_user=args.db_user,
            db_name=args.db_name,
        )
    except Exception as e:
        print(f"ERROR: Could not fetch Kite credentials: {e}")
        return 1

    try:
        kite = KiteConnect(api_key=api_key)
        kite.set_access_token(access_token)
        trades = kite.trades()
    except Exception as e:
        print(f"ERROR: Kite /trades call failed: {e}")
        return 2

    print("SUCCESS: Kite /trades call completed")
    print(f"trade_count={len(trades)}")

    if args.print_first:
        if trades:
            print(json.dumps(trades[0], indent=2, default=str))
        else:
            print("No trades returned.")

    if args.save_json:
        with open(args.save_json, "w", encoding="utf-8") as f:
            json.dump(trades, f, indent=2, default=str)
        print(f"Saved JSON to {args.save_json}")

    if args.save_csv:
        save_csv(trades, args.save_csv)
        print(f"Saved CSV to {args.save_csv}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
