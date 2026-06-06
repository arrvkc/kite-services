from modules.chrome_applescript import run_js, click_by_exact_text, wait_ms

def verify_logged_in():
    js = r"""
(() => {
  const body = document.body.innerText || '';
  if (body.includes('Logout') || body.includes('Chakravarthi Akiri')) return 'LOGGED_IN';
  if (body.includes('Continue with Google') || body.includes('Login')) return 'NOT_LOGGED_IN';
  return 'UNKNOWN';
})()
"""
    return run_js(js)

def select_account(account_name="Eajee"):
    js = f"""
(() => {{
  const account = [...document.querySelectorAll('a,span,div')]
    .find(x => (x.innerText || '').trim() === '{account_name}');
  if (!account) return 'ACCOUNT_NOT_FOUND';
  account.click();
  return 'ACCOUNT_SELECTED: {account_name}';
}})()
"""
    return run_js(js)

def click_mojoscore_tab():
    return click_by_exact_text("a", "MOJOSCORE")

def ensure_mojoscore():
    result = click_mojoscore_tab()
    wait_ms(1500)
    return result

def inspect_status():
    js = r"""
(() => {
  const activeTab = [...document.querySelectorAll('li.tab.active, .tab.active, li.active')]
    .map(x => (x.innerText || '').trim())
    .filter(Boolean);

  const body = document.body.innerText || '';

  return JSON.stringify({
    url: location.href,
    logged_in_hint: body.includes('Logout') || body.includes('Chakravarthi Akiri'),
    has_eajee: body.includes('Eajee'),
    has_mojoscore: body.includes('MOJOSCORE'),
    active_tabs: activeTab
  }, null, 2);
})()
"""
    return run_js(js)








def click_show_more_until_done(max_clicks=100, wait_seconds=2, reappear_timeout=20):
    import time

    count = 0

    js_visible = r"""
(() => {
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
})()
"""

    js_click = r"""
(() => {
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
})()
"""

    for i in range(max_clicks):
        if run_js(js_visible) != "VISIBLE":
            return f"SHOW_MORE_DONE: clicked {count} times"

        result = run_js(js_click)

        if result != "CLICKED_SHOW_MORE":
            return f"SHOW_MORE_DONE: clicked {count} times"

        count += 1
        print(f"Show More clicked: {count}")

        # Wait for fetch/render cycle. The button may disappear and reappear.
        time.sleep(wait_seconds)

        appeared_again = False
        deadline = time.time() + reappear_timeout

        while time.time() < deadline:
            visible = run_js(js_visible)

            if visible == "VISIBLE":
                appeared_again = True
                break

            time.sleep(1)

        if not appeared_again:
            return f"SHOW_MORE_DONE: clicked {count} times"

    return f"SHOW_MORE_MAX_REACHED: clicked {count} times"
