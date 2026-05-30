import json
import time
from pathlib import Path

import pyotp

from .run_context import RUN_DIR


def load_zerodha_credentials(user_id):
    path = Path.home() / "kite_services" / "secrets" / f"zerodha_{user_id}.json"

    if not path.exists():
        raise RuntimeError(f"Credential file not found: {path}")

    data = json.loads(path.read_text())

    required = [
        "zerodha_user_id",
        "zerodha_password",
        "zerodha_totp_hash",
    ]

    missing = [key for key in required if not data.get(key)]

    if missing:
        raise RuntimeError(f"Missing Zerodha credentials: {missing}")

    return data


def complete_zerodha_login(page, user_id):
    creds = load_zerodha_credentials(user_id)

    page.wait_for_timeout(2000)

    page.screenshot(
        path=str(RUN_DIR / "07_zerodha_login_page.png"),
        full_page=True,
    )

    print("Zerodha page title:", page.title())
    print("Zerodha page URL:", page.url)

    page.fill("input#userid", creds["zerodha_user_id"])
    page.fill("input#password", creds["zerodha_password"])

    page.screenshot(
        path=str(RUN_DIR / "08_zerodha_credentials_filled.png"),
        full_page=True,
    )

    page.click("button[type='submit']")

    page.wait_for_timeout(2000)

    page.screenshot(
        path=str(RUN_DIR / "09_zerodha_totp_page.png"),
        full_page=True,
    )

    totp = pyotp.TOTP(creds["zerodha_totp_hash"]).now()

    totp_selectors = [
        "input[type='text']",
        "input[type='number']",
        "input#userid",
        "input",
    ]

    filled = False

    for selector in totp_selectors:
        locator = page.locator(selector)
        if locator.count() > 0:
            locator.first.fill(totp)
            filled = True
            print(f"Filled TOTP using selector: {selector}")
            break

    if not filled:
        raise RuntimeError("Could not find Zerodha TOTP input")

    page.keyboard.press("Enter")

    page.wait_for_timeout(5000)

    page.screenshot(
        path=str(RUN_DIR / "10_after_zerodha_totp_submit.png"),
        full_page=True,
    )

    print("After Zerodha login URL:", page.url)

    return page.url
