from pathlib import Path
import os
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

load_dotenv()

EAJEE_URL = os.getenv("EAJEE_URL", "https://eajee.in").rstrip("/")
EAJEE_USERNAME = os.getenv("EAJEE_USERNAME", "").strip()
EAJEE_PASSWORD = os.getenv("EAJEE_PASSWORD", "").strip()
EAJEE_PROFILE_DIR = Path(os.getenv("EAJEE_PROFILE_DIR", "server_runner/profile_eajee"))

def latest_mojo_html():
    files = list(Path("saved_pages").glob("marketsmojo_server_mojoscore_*.html"))
    if not files:
        raise FileNotFoundError("No MarketsMojo server HTML file found in saved_pages/")
    return sorted(files, key=lambda x: x.stat().st_mtime)[-1]

def login_if_required(page):
    page.goto(EAJEE_URL, wait_until="domcontentloaded")
    page.wait_for_timeout(2000)

    body = page.inner_text("body") if page.locator("body").count() else ""

    if "Logout" in body or "Dashboard" in body or "AlgoSmart" in body:
        return "ALREADY_LOGGED_IN"

    if not EAJEE_USERNAME or not EAJEE_PASSWORD:
        raise RuntimeError("EAJEE_USERNAME / EAJEE_PASSWORD missing in .env")

    email_input = page.locator('input[type="email"], input[name="email"], input[name="username"], input[type="text"]').first
    password_input = page.locator('input[type="password"]').first

    email_input.fill(EAJEE_USERNAME)
    password_input.fill(EAJEE_PASSWORD)

    page.locator('button:has-text("Login"), button:has-text("Sign in"), input[type="submit"]').first.click()
    page.wait_for_timeout(5000)

    body = page.inner_text("body") if page.locator("body").count() else ""

    if "Logout" not in body and "AlgoSmart" not in body and "Dashboard" not in body:
        raise RuntimeError("Eajee login may have failed. Check opened browser.")

    return "LOGIN_SUCCESS"

html_file = latest_mojo_html()
print("Uploading:", html_file)

with sync_playwright() as p:
    context = p.chromium.launch_persistent_context(
        user_data_dir=str(EAJEE_PROFILE_DIR),
        headless=False,
        channel="chrome",
        viewport={"width": 1400, "height": 900},
    )

    page = context.pages[0] if context.pages else context.new_page()

    print("Login status:", login_if_required(page))

    page.goto(f"{EAJEE_URL}/data/mu", wait_until="domcontentloaded")
    page.wait_for_timeout(3000)

    print("Current URL:", page.url)
    print("Title:", page.title())

    body_text = page.inner_text("body") if page.locator("body").count() else ""
    print("Body preview:", body_text[:300])

    if "Internal Server Error" in body_text:
        raise RuntimeError("Eajee returned Internal Server Error after login when opening /data/mu.")

    file_inputs = page.locator('input[type="file"]')
    print("File input count:", file_inputs.count())

    if file_inputs.count() == 0:
        page.screenshot(path="screenshots/eajee_no_file_input.png", full_page=True)
        raise RuntimeError("No file input found on Eajee upload page.")

    file_inputs.first.set_input_files(str(html_file.resolve()))
    print("File selected.")

    page.locator("text=Import").first.click()

    page.wait_for_timeout(5000)

    for _ in range(120):
        if "/data/instrument_list" in page.url:
            print("UPLOAD_SUCCESS")
            print("Final URL:", page.url)
            break

        body_text = page.inner_text("body") if page.locator("body").count() else ""

        if "Internal Server Error" in body_text:
            print("UPLOAD_FAILED_INTERNAL_SERVER_ERROR")
            print("Final URL:", page.url)
            page.screenshot(path="screenshots/eajee_upload_internal_server_error.png", full_page=True)
            raise RuntimeError("Eajee returned Internal Server Error after import.")

        page.wait_for_timeout(1000)

    else:
        page.screenshot(path="screenshots/eajee_upload_timeout.png", full_page=True)
        raise RuntimeError(f"Upload did not reach instrument_list. Final URL: {page.url}")

    context.close()
