from .run_context import RUN_DIR
import time

from .config import (
    LOGIN_URL,
    TARGET_URL,
    USERNAME,
    PASSWORD,
    SCREENSHOT_DIR,
)


def safe_screenshot(page, name):
    page.screenshot(
        path=str(SCREENSHOT_DIR / f"{name}_{int(time.time())}.png"),
        full_page=True,
    )


def login_and_open_target(page):
    print(f"Opening login page: {LOGIN_URL}")

    page.goto(
        LOGIN_URL,
        wait_until="domcontentloaded",
        timeout=60000,
    )

    safe_screenshot(page, "01_login_page")

    print("Page title:", page.title())
    print("Current URL:", page.url)

    # Try common username selectors
    username_selectors = [
        "input[name='username']",
        "input#username",
        "input[name='email']",
        "input#email",
        "input[name='userid']",
        "input#userid",
        "input[type='text']",
    ]

    password_selectors = [
        "input[name='password']",
        "input#password",
        "input[type='password']",
    ]

    username_filled = False
    for selector in username_selectors:
        if page.locator(selector).count() > 0:
            print(f"Using username selector: {selector}")
            page.fill(selector, USERNAME)
            username_filled = True
            break

    if not username_filled:
        raise RuntimeError("Could not find username field")

    password_filled = False
    for selector in password_selectors:
        if page.locator(selector).count() > 0:
            print(f"Using password selector: {selector}")
            page.fill(selector, PASSWORD)
            password_filled = True
            break

    if not password_filled:
        raise RuntimeError("Could not find password field")

    safe_screenshot(page, "02_filled_login_form")

    submit_selectors = [
        "button[type='submit']",
        "input[type='submit']",
        "button",
    ]

    clicked = False
    for selector in submit_selectors:
        if page.locator(selector).count() > 0:
            print(f"Using submit selector: {selector}")
            page.click(selector)
            clicked = True
            break

    if not clicked:
        raise RuntimeError("Could not find submit button")

    page.wait_for_load_state(
        "domcontentloaded",
        timeout=60000,
    )

    page.wait_for_timeout(3000)

    safe_screenshot(page, "03_after_login")

    print(f"Opening target page: {TARGET_URL}")

    page.goto(
        TARGET_URL,
        wait_until="domcontentloaded",
        timeout=60000,
    )

    page.wait_for_timeout(3000)

    safe_screenshot(page, "04_target_page")

    print(f"SUCCESS CURRENT URL: {page.url}")
