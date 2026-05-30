import pyotp

from .credentials import get_zerodha_credentials
from .run_context import RUN_DIR


def complete_zerodha_login(page, user_id, artifact_dir=None):
    creds = get_zerodha_credentials(user_id)

    if artifact_dir is None:
        artifact_dir = RUN_DIR

    page.wait_for_timeout(2000)

    page.screenshot(
        path=str(artifact_dir / "07_zerodha_login_page.png"),
        full_page=True,
    )

    print("Zerodha page title:", page.title())
    print("Zerodha page URL:", page.url)

    page.fill("input#userid", creds["zerodha_user_id"])
    page.fill("input#password", creds["zerodha_password"])

    page.screenshot(
        path=str(artifact_dir / "08_zerodha_credentials_filled.png"),
        full_page=True,
    )

    page.click("button[type='submit']")

    page.wait_for_timeout(2000)

    page.screenshot(
        path=str(artifact_dir / "09_zerodha_totp_page.png"),
        full_page=True,
    )

    totp = pyotp.TOTP(creds["zerodha_totp_hash"]).now()

    selectors = [
        "input[type='text']",
        "input[type='number']",
        "input",
    ]

    for selector in selectors:
        locator = page.locator(selector)
        if locator.count() > 0:
            locator.first.fill(totp)
            print(f"Filled TOTP using selector: {selector}")
            break
    else:
        raise RuntimeError("Zerodha TOTP input not found")

    page.keyboard.press("Enter")

    page.wait_for_timeout(5000)

    page.screenshot(
        path=str(artifact_dir / "10_after_zerodha_totp_submit.png"),
        full_page=True,
    )

    print("After Zerodha login URL:", page.url)

    return page.url
