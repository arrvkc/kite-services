from pathlib import Path
from datetime import datetime
from modules.chrome_applescript import run_js

def save_html(prefix="marketsmojo_mojoscore"):
    Path("saved_pages").mkdir(exist_ok=True)

    js = r"""
(() => document.documentElement.outerHTML)()
"""
    html = run_js(js)

    filename = f"{prefix}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
    path = Path("saved_pages") / filename
    path.write_text(html, encoding="utf-8")

    return str(path)
