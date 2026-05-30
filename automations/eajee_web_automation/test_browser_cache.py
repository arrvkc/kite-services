from .browser import create_browser
from .config import LOGIN_URL
from .run_context import RUN_DIR, zip_run


def main():
    playwright, browser, context, page = create_browser(headless=False)

    try:
        print("Opening fresh browser context")

        cookies = context.cookies()

        print("Initial cookies count:", len(cookies))
        print("Initial cookies:", cookies)

        page.goto(
            LOGIN_URL,
            wait_until="domcontentloaded",
            timeout=60000,
        )

        print("URL:", page.url)
        print("Title:", page.title())

        local_storage = page.evaluate(
            "() => JSON.stringify(window.localStorage)"
        )

        session_storage = page.evaluate(
            "() => JSON.stringify(window.sessionStorage)"
        )

        print("Local storage:", local_storage)
        print("Session storage:", session_storage)

        page.screenshot(
            path=str(RUN_DIR / "01_fresh_login_page.png"),
            full_page=True,
        )

        cookies_after_load = context.cookies()

        print(
            "Cookies after loading login page:",
            cookies_after_load,
        )

        page.screenshot(
            path=str(RUN_DIR / "02_after_login_page_load.png"),
            full_page=True,
        )

    finally:
        zip_run()

        context.close()
        browser.close()
        playwright.stop()


if __name__ == "__main__":
    main()
