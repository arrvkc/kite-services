from pathlib import Path
from playwright.sync_api import sync_playwright

PROFILE_DIR = Path("server_runner/profile")
PROFILE_DIR.mkdir(parents=True, exist_ok=True)

URL = "https://www.marketsmojo.com/portfolio-plus/watchlist"

with sync_playwright() as p:
    context = p.chromium.launch_persistent_context(
        user_data_dir=str(PROFILE_DIR),
        headless=False,
        channel="chrome",
        viewport={"width": 1400, "height": 900},
    )

    page = context.pages[0] if context.pages else context.new_page()
    page.goto(URL, wait_until="networkidle")

    print("Browser opened with persistent profile.")
    print("Login manually if required.")
    input("After MarketsMojo is fully logged in, press ENTER here...")

    print("Current URL:", page.url)
    print("Session/profile saved in:", PROFILE_DIR)

    context.close()
