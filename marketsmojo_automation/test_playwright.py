from playwright.sync_api import sync_playwright

with sync_playwright() as p:
    browser = p.chromium.launch(headless=False)
    page = browser.new_page()
    page.goto("https://www.marketsmojo.com")
    input("Login manually in the browser, then press ENTER here...")
    browser.close()
