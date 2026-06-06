from pathlib import Path
import os
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

load_dotenv()

EAJEE_URL = os.getenv("EAJEE_URL", "https://eajee.in").rstrip("/")
EAJEE_PROFILE_DIR = Path(os.getenv("EAJEE_PROFILE_DIR", "server_runner/profile_eajee"))

EAJEE_PROFILE_DIR.mkdir(parents=True, exist_ok=True)

with sync_playwright() as p:
    context = p.chromium.launch_persistent_context(
        user_data_dir=str(EAJEE_PROFILE_DIR),
        headless=False,
        channel="chrome",
        viewport={"width": 1400, "height": 900},
    )

    page = context.pages[0] if context.pages else context.new_page()
    page.goto(EAJEE_URL, wait_until="domcontentloaded")

    print("Eajee opened.")
    print("Login manually if required.")
    input("After Eajee is fully logged in, press ENTER here...")

    page.goto(f"{EAJEE_URL}/data/mu", wait_until="domcontentloaded")
    page.wait_for_timeout(3000)

    print("Current URL:", page.url)
    print("Eajee session saved in:", EAJEE_PROFILE_DIR)

    context.close()
