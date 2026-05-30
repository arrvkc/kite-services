import argparse
import csv
import traceback
import time

from .browser import create_browser
from .login import login_and_open_target
from .config import BASE_DIR
from .run_context import RUN_DIR, zip_run
from .zerodha_login import complete_zerodha_login


LOG_FILE = BASE_DIR / "logs" / "eajee_web_automation" / "refresh_token_results.csv"


def log_result(row):
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)

    file_exists = LOG_FILE.exists()

    with LOG_FILE.open("a", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "timestamp",
                "user_id",
                "mode",
                "status",
                "stage",
                "refresh_url",
                "final_url",
                "error",
                "traceback_file",
                "run_dir",
                "zip_file",
            ],
        )

        if not file_exists:
            writer.writeheader()

        writer.writerow(row)


def save_traceback(exc):
    traceback_file = RUN_DIR / "error_traceback.txt"

    traceback_file.write_text(
        "".join(
            traceback.format_exception(
                type(exc),
                exc,
                exc.__traceback__,
            )
        )
    )

    return str(traceback_file)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("user_id")
    parser.add_argument("--headless", action="store_true")
    args = parser.parse_args()

    target_user_id = args.user_id
    mode = "HEADLESS" if args.headless else "VISIBLE"

    playwright, browser, context, page = create_browser(
        headless=args.headless
    )

    refresh_url = ""
    status = "FAILED"
    stage = "START"
    error = ""
    traceback_file = ""

    try:
        print(f"RUN_DIR={RUN_DIR}")
        print(f"USER={target_user_id}")
        print(f"MODE={mode}")

        stage = "EAJEE_LOGIN_AND_OPEN_USERS_PAGE"
        login_and_open_target(page)

        stage = "FIND_USER_ROW"
        row = page.locator("tr", has_text=target_user_id).first

        if row.count() == 0:
            raise RuntimeError(f"User row not found: {target_user_id}")

        stage = "FIND_REFRESH_LINK"
        refresh_link = row.locator(
            "a[title='Connect / Refresh Token']"
        ).first

        if refresh_link.count() == 0:
            raise RuntimeError(
                f"Refresh link not found for user: {target_user_id}"
            )

        refresh_url = refresh_link.get_attribute("href") or ""

        page.screenshot(
            path=str(RUN_DIR / "05_before_refresh.png"),
            full_page=True,
        )

        stage = "CLICK_REFRESH_LINK"
        print(f"Clicking refresh link: {refresh_url}")

        with page.expect_navigation(timeout=60000):
            refresh_link.click()

        page.wait_for_timeout(3000)

        final_url = page.url

        page.screenshot(
            path=str(RUN_DIR / "06_after_refresh.png"),
            full_page=True,
        )

        print(f"After refresh click URL: {final_url}")

        if "kite.zerodha.com" in final_url:
            stage = "ZERODHA_LOGIN"
            complete_zerodha_login(
                page,
                target_user_id,
            )

        stage = "VERIFY_FINAL_RESULT"

        page.wait_for_timeout(2000)

        final_text = page.locator("body").inner_text()

        (RUN_DIR / "final_page_text.txt").write_text(final_text)

        if "Kite token updated" in final_text or "Success" in final_text:
            status = "SUCCESS"
        else:
            status = "UNKNOWN_FINAL_STATE"
            raise RuntimeError(
                "Refresh flow completed, but success message was not found"
            )

        stage = "DONE"

        print(f"SUCCESS: Token refresh completed for {target_user_id}")
        print(f"FINAL_URL={page.url}")

    except Exception as exc:
        error = str(exc)
        traceback_file = save_traceback(exc)

        try:
            page.screenshot(
                path=str(RUN_DIR / "error_page.png"),
                full_page=True,
            )
            (RUN_DIR / "error_page_text.txt").write_text(
                page.locator("body").inner_text()
            )
        except Exception:
            pass

        print(f"FAILED at stage: {stage}")
        print(f"ERROR: {error}")
        print(f"TRACEBACK_FILE={traceback_file}")

    finally:
        zip_file = zip_run()

        log_result(
            {
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "user_id": target_user_id,
                "mode": mode,
                "status": status,
                "stage": stage,
                "refresh_url": refresh_url,
                "final_url": page.url if page else "",
                "error": error,
                "traceback_file": traceback_file,
                "run_dir": str(RUN_DIR),
                "zip_file": str(zip_file),
            }
        )

        context.close()
        browser.close()
        playwright.stop()


if __name__ == "__main__":
    main()
