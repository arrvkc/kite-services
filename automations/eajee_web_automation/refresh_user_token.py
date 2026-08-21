import argparse
import csv
import traceback
import time
from urllib.parse import urlsplit

from .browser import create_browser
from .login import login_and_open_target
from .config import BASE_DIR
from .diagnostic_urls import safe_diagnostic_text, safe_url_for_log
from .run_context import RUN_DIR, zip_run
from .zerodha_login import complete_zerodha_login
from .persisted_refresh_verification import (
    DONE,
    SUCCESS,
    UNKNOWN_FINAL_STATE,
    locate_target_user_state,
    verify_persisted_refresh,
)


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

        safe_row = dict(row)
        safe_row["refresh_url"] = safe_url_for_log(row.get("refresh_url", ""))
        safe_row["final_url"] = safe_url_for_log(row.get("final_url", ""))
        safe_row["error"] = safe_diagnostic_text(row.get("error", ""))
        writer.writerow(safe_row)


def save_traceback(exc):
    traceback_file = RUN_DIR / "error_traceback.txt"

    traceback_file.write_text(
        safe_diagnostic_text(
            "".join(
                traceback.format_exception(
                    type(exc),
                    exc,
                    exc.__traceback__,
                )
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
        pre_refresh_state = locate_target_user_state(page, target_user_id)
        pre_refresh_updated_on = pre_refresh_state.updated_on
        row = pre_refresh_state.row

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
        print(f"Clicking refresh link: {safe_url_for_log(refresh_url)}")

        with page.expect_navigation(timeout=60000):
            refresh_link.click()

        page.wait_for_timeout(3000)

        final_url = page.url

        page.screenshot(
            path=str(RUN_DIR / "06_after_refresh.png"),
            full_page=True,
        )

        print(f"After refresh click URL: {safe_url_for_log(final_url)}")

        if "kite.zerodha.com" in final_url:
            stage = "ZERODHA_LOGIN"
            complete_zerodha_login(
                page,
                target_user_id,
            )

        stage = "VERIFY_FINAL_RESULT"
        status = UNKNOWN_FINAL_STATE

        try:
            page.wait_for_url("**/data/users*", timeout=60000)
            page.locator("#users-table").wait_for(
                state="visible",
                timeout=60000,
            )
        except Exception:
            final_path = urlsplit(page.url).path or "/"
            raise RuntimeError(
                "Expected EAJEE users page state was not reached; "
                f"current path is {final_path}"
            ) from None

        final_text = page.locator("body").inner_text()

        (RUN_DIR / "final_page_text.txt").write_text(final_text)

        verification = verify_persisted_refresh(
            page,
            target_user_id,
            pre_refresh_updated_on,
        )
        status = verification.status
        if status != SUCCESS:
            raise RuntimeError(verification.reason)

        stage = DONE

        print(f"SUCCESS: Token refresh completed for {target_user_id}")
        print(f"FINAL_URL={safe_url_for_log(page.url)}")

    except Exception as exc:
        error = safe_diagnostic_text(exc)
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
