from __future__ import annotations

import argparse

from kiteconnect import KiteConnect

from services.kite_credentials_service import get_kite_credentials


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Validate read-only Kite credentials for Strategy recovery."
    )
    parser.add_argument("user_id")
    return parser


def main() -> None:
    args = build_argument_parser().parse_args()
    try:
        api_key, access_token = get_kite_credentials(args.user_id)
        kite = KiteConnect(api_key=api_key)
        kite.set_access_token(access_token)
        profile = kite.profile()
    except Exception as exc:
        raise SystemExit(
            f"KITE_CREDENTIAL_STATUS=FAIL reason=credential_validation "
            f"error_class={type(exc).__name__}"
        ) from None
    actual_user_id = str(profile.get("user_id") or "").strip().upper()
    if actual_user_id != args.user_id.strip().upper():
        raise SystemExit("KITE_CREDENTIAL_STATUS=FAIL reason=wrong_account")
    print("KITE_CREDENTIAL_STATUS=PASS")


if __name__ == "__main__":
    main()
