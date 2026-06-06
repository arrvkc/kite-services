from pathlib import Path
from datetime import datetime
from playwright.sync_api import sync_playwright

PROFILE_DIR = Path("server_runner/profile")
SAVED_DIR = Path("saved_pages")
SAVED_DIR.mkdir(exist_ok=True)

URL = "https://www.marketsmojo.com/portfolio-plus/watchlist"


def ensure_account_eajee(page):
    # Open account dropdown
    result = page.evaluate(
        """
        () => {
          const dropdown = [...document.querySelectorAll('[ng-click="$parent.showaccountlist = !$parent.showaccountlist"], .addbuttonnew22')]
            .find(x => x.offsetParent !== null);

          if (!dropdown) return 'DROPDOWN_NOT_FOUND';

          dropdown.scrollIntoView({block: 'center'});
          dropdown.click();

          return 'DROPDOWN_OPENED';
        }
        """
    )

    page.wait_for_timeout(1000)

    # Click exact Eajee option inside dropdown
    result2 = page.evaluate(
        """
        () => {
          const eajee = [...document.querySelectorAll('a,div,span')]
            .find(x => (x.innerText || '').trim() === 'Eajee');

          if (!eajee) return 'EAJEE_OPTION_NOT_FOUND';

          eajee.scrollIntoView({block: 'center'});
          eajee.click();

          return 'EAJEE_SELECTED';
        }
        """
    )

    page.wait_for_timeout(5000)

    # Verify actual selected account / page text
    verify = page.evaluate(
        """
        () => {
          const body = document.body.innerText || '';
          const selected = [...document.querySelectorAll('.addbuttonnew22, .user-onboardtoptxt, .ng-binding')]
            .map(x => (x.innerText || '').trim())
            .filter(Boolean);

          return JSON.stringify({
            has_eajee: body.includes('Eajee'),
            selected_candidates: selected.slice(0, 20)
          });
        }
        """
    )

    return result + ' -> ' + result2 + ' -> VERIFY ' + verify


def click_mojoscore_tab(page):
    return page.evaluate(
        """
        () => {
          const el = [...document.querySelectorAll('a')]
            .find(x =>
              (x.innerText || '').trim() === 'MOJOSCORE' &&
              (x.getAttribute('ng-click') || '').replace(/\\s+/g, ' ').trim() === 'tab = 5;'
            );

          if (!el) return 'MOJOSCORE_TAB_NOT_FOUND';

          el.scrollIntoView({block: 'center'});
          el.click();

          return 'MOJOSCORE_TAB_CLICKED';
        }
        """
    )


def click_show_more_until_done(page, max_clicks=100, wait_seconds=2, reappear_timeout=20):
    count = 0

    js_visible = """
    () => {
      const buttons = [...document.querySelectorAll('a.btn.btn-info.showmore, a.showmore, a')]
        .filter(x => (x.innerText || '').trim() === 'Show More')
        .filter(x => {
          const style = window.getComputedStyle(x);
          const rect = x.getBoundingClientRect();
          return style.display !== 'none' &&
                 style.visibility !== 'hidden' &&
                 rect.width > 0 &&
                 rect.height > 0;
        });

      return buttons.length ? 'VISIBLE' : 'NOT_VISIBLE';
    }
    """

    js_click = """
    () => {
      const buttons = [...document.querySelectorAll('a.btn.btn-info.showmore, a.showmore, a')]
        .filter(x => (x.innerText || '').trim() === 'Show More')
        .filter(x => {
          const style = window.getComputedStyle(x);
          const rect = x.getBoundingClientRect();
          return style.display !== 'none' &&
                 style.visibility !== 'hidden' &&
                 rect.width > 0 &&
                 rect.height > 0;
        });

      if (!buttons.length) return 'NO_SHOW_MORE';

      const btn = buttons[buttons.length - 1];
      btn.scrollIntoView({block: 'center'});
      btn.click();

      return 'CLICKED_SHOW_MORE';
    }
    """

    for _ in range(max_clicks):

        visible = page.evaluate(js_visible)

        if visible != "VISIBLE":
            return count

        result = page.evaluate(js_click)

        if result != "CLICKED_SHOW_MORE":
            return count

        count += 1
        print(f"Show More clicked: {count}")

        page.wait_for_timeout(wait_seconds * 1000)

        appeared_again = False
        deadline = reappear_timeout

        for _ in range(deadline):
            visible_after = page.evaluate(js_visible)

            if visible_after == "VISIBLE":
                appeared_again = True
                break

            page.wait_for_timeout(1000)

        if not appeared_again:
            return count

    return count


with sync_playwright() as p:
    context = p.chromium.launch_persistent_context(
        user_data_dir=str(PROFILE_DIR),
        headless=False,
        channel="chrome",
        viewport={"width": 1920, "height": 1080},
        device_scale_factor=1,
        is_mobile=False,
        has_touch=False,
        user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36",
    )

    page = context.pages[0] if context.pages else context.new_page()
    page.goto(URL, wait_until="domcontentloaded")
    page.wait_for_timeout(8000)

    body = page.inner_text("body")

    if "Existing User" in body and "Chakravarthi Akiri" not in body:
        raise RuntimeError("Not logged in. Login manually once using test_persistent_profile.py.")

    print("Logged in.")
    print("Current URL:", page.url)

    print("Ensuring Eajee account...")
    account_result = ensure_account_eajee(page)
    print(account_result)

    if '"has_eajee":true' not in account_result:
        raise RuntimeError("Eajee account was not selected. Stopping before saving wrong HTML.")

    page.wait_for_timeout(3000)

    print("Clicking MOJOSCORE...")
    print(click_mojoscore_tab(page))
    page.wait_for_timeout(3000)

    print("Clicking Show More until done...")
    clicks = click_show_more_until_done(page)
    print(f"Total Show More clicks: {clicks}")

    final_text = page.inner_text("body")
    if "Eajee" not in final_text:
        raise RuntimeError("Eajee not found in final page. Refusing to save wrong account HTML.")

    html = page.evaluate("() => document.documentElement.outerHTML")

    filename = f"marketsmojo_server_mojoscore_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
    path = SAVED_DIR / filename
    path.write_text(html, encoding="utf-8")

    print("Saved HTML:", path)

    context.close()
