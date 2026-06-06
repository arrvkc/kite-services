from pathlib import Path
from playwright.sync_api import sync_playwright

PROFILE_DIR = Path("server_runner/profile")

with sync_playwright() as p:
    context = p.chromium.launch_persistent_context(
        user_data_dir=str(PROFILE_DIR),
        headless=False,
        channel="chrome",
    )

    page = context.pages[0] if context.pages else context.new_page()

    page.goto(
        "https://www.marketsmojo.com/portfolio-plus/watchlist",
        wait_until="domcontentloaded"
    )

    page.wait_for_timeout(5000)

    result = page.evaluate("""
    () => {
      return [...document.querySelectorAll('*')]
        .filter(el => (el.innerText || '').trim() === 'Eajee')
        .map(el => ({
            tag: el.tagName,
            className: el.className,
            ngClick: el.getAttribute('ng-click'),
            href: el.getAttribute('href')
        }));
    }
    """)

    print(result)

    input("Press ENTER...")
    context.close()
