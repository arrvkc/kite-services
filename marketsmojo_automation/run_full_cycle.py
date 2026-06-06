from pathlib import Path
import traceback

from modules.chrome_applescript import open_url, wait_ms, get_url
from modules.marketsmojo_actions import (
    verify_logged_in,
    select_account,
    ensure_mojoscore,
    click_show_more_until_done,
    inspect_status,
)
from modules.html_saver import save_html
from modules.uploader import upload_html

MARKETSMOJO_URL = "https://www.marketsmojo.com/portfolio-plus/watchlist"

def main():
    Path("logs").mkdir(exist_ok=True)
    Path("screenshots").mkdir(exist_ok=True)

    try:
        open_url(MARKETSMOJO_URL)
        wait_ms(4000)

        print("Current URL:", get_url())

        login_status = verify_logged_in()
        print("Login status:", login_status)

        if login_status == "NOT_LOGGED_IN":
            raise RuntimeError("Please login manually in Chrome first, then rerun this script.")

        print("Selecting Eajee...")
        print(select_account("Eajee"))
        wait_ms(1500)

        print("Clicking MOJOSCORE...")
        print(ensure_mojoscore())

        print("Clicking Show More until done...")
        print(click_show_more_until_done())

        print("Status:")
        print(inspect_status())

        html_path = save_html()
        print("Saved HTML:", html_path)

        upload_response = upload_html(html_path)
        print("Upload response:", upload_response)

        print("DONE")

    except Exception as e:
        with open("logs/error.log", "a", encoding="utf-8") as f:
            f.write("\n\n--- ERROR ---\n")
            f.write(str(e))
            f.write("\n")
            f.write(traceback.format_exc())
        raise

if __name__ == "__main__":
    main()
