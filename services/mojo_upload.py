import re
from playwright.sync_api import Playwright, sync_playwright, expect


def run(playwright: Playwright) -> None:
    browser = playwright.chromium.launch(headless=False)
    context = browser.new_context()
    page = context.new_page()
    page.goto("https://www.marketsmojo.com/mojofeed/login")
    page.get_by_role("textbox", name="Enter your email").click()
    page.get_by_role("textbox", name="Enter your email").fill("arrvkc@gmail.com")
    page.get_by_role("textbox", name="Enter your email").press("Tab")
    page.get_by_role("textbox", name="Enter your password").fill("h6crzb4k")
    page.get_by_role("textbox", name="Enter your password").press("Enter")
    page.get_by_role("button", name="Sign In").click()
    page.get_by_role("link", name="Your Watchlist marketsmojo").click()
    page.get_by_role("link", name="AMFI - M... ").click()
    page.get_by_role("link", name="Eajee").click()
    page.get_by_role("link", name="MOJOSCORE").click()
    page.get_by_role("link", name="Show More").click()
    page.get_by_role("link", name="Show More").click()




with sync_playwright() as playwright:
    run(playwright)
