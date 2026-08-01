#!/usr/bin/env python3

import os
import sys

os.environ["KITE_CREDENTIALS_MODE"] = "local"

from kiteconnect import KiteConnect
from services.kite_credentials_service import get_kite_credentials

if len(sys.argv) < 2:
    print("Usage:")
    print("  delete_all_gtts.py USER1 [USER2 USER3 ...]")
    sys.exit(1)

for user_id in sys.argv[1:]:

    print(f"\n{'=' * 80}")
    print(f"USER: {user_id}")
    print(f"{'=' * 80}")

    try:
        api_key, access_token = get_kite_credentials(user_id)

        kite = KiteConnect(api_key=api_key)
        kite.set_access_token(access_token)

        gtts = kite.get_gtts()

        print(f"Found {len(gtts)} GTTs")

        for gtt in gtts:
            trigger_id = int(gtt["id"])

            try:
                kite.delete_gtt(trigger_id)
                print(f"Deleted trigger_id={trigger_id}")
            except Exception as e:
                print(f"Failed trigger_id={trigger_id}: {e}")

    except Exception as e:
        print(f"Failed user {user_id}: {e}")

print("\nCompleted")
