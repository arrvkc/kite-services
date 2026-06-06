from playwright.sync_api import sync_playwright
from pathlib import Path

Path("sessions").mkdir(exist_ok=True)

with sync_playwright() as p:
    browser = p.chromium.launch(
        channel="chrome",
        headless=False
    )

    context = browser.new_context()
    page = context.new_page()

    page.goto("https://www.marketsmojo.com")

    input("Login completely, then press ENTER here...")

    context.storage_state(path="sessions/marketsmojo_state.json")

    print("Session saved.")

    browser.close()
