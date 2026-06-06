import subprocess
import json
import time

def run_js(js: str) -> str:
    js_literal = json.dumps(js)
    script = f'''
tell application "Google Chrome"
    set resultText to execute active tab of front window javascript {js_literal}
end tell
return resultText
'''
    return subprocess.check_output(["osascript", "-e", script]).decode("utf-8").strip()

def get_url() -> str:
    script = 'tell application "Google Chrome" to get URL of active tab of front window'
    return subprocess.check_output(["osascript", "-e", script]).decode("utf-8").strip()

def open_url(url: str):
    script = f'''
tell application "Google Chrome"
    activate
    open location "{url}"
end tell
'''
    subprocess.check_call(["osascript", "-e", script])
    time.sleep(4)

def click_by_exact_text(tag: str, text: str) -> str:
    js = f"""
(() => {{
  const el = [...document.querySelectorAll({json.dumps(tag)})]
    .find(x => (x.innerText || '').trim() === {json.dumps(text)});
  if (!el) return 'NOT_FOUND: {text}';
  el.click();
  return 'CLICKED: {text}';
}})()
"""
    return run_js(js)

def wait_ms(ms: int):
    time.sleep(ms / 1000)
    return f"WAITED {ms}ms"
