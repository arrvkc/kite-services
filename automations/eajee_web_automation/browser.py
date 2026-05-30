from playwright.sync_api import sync_playwright


def create_browser(headless=False):
    playwright = sync_playwright().start()

    browser = playwright.chromium.launch(
        headless=headless
    )

    context = browser.new_context(
        viewport={
            "width": 1440,
            "height": 900,
        }
    )

    page = context.new_page()

    return playwright, browser, context, page
