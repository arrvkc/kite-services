import re
from playwright.sync_api import Playwright, sync_playwright, expect


def run(playwright: Playwright) -> None:
    browser = playwright.chromium.launch(headless=False)
    context = browser.new_context()
    page = context.new_page()
    page.goto("https://eajee.in/login")
    page.get_by_role("textbox", name="Username").click()
    page.get_by_role("textbox", name="Username").fill("eajee_admin")
    page.get_by_role("textbox", name="Username").press("Tab")
    page.get_by_role("textbox", name="Password").fill("welcome@123")
    page.get_by_role("button", name="Sign In").click()
    page.goto("https://eajee.in/data/upload52")
    page.get_by_role("button", name="Fetch Both").click()
    page.wait_for_load_state("networkidle")
    page.screenshot(path="fetch52_result.png")
    page.get_by_role("link", name=" Logout").click()

    # ---------------------
    context.close()
    browser.close()


with sync_playwright() as playwright:
    run(playwright)
