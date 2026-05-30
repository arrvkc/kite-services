import argparse
import time

from .browser import create_browser
from .login import login_and_open_target


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--keep-open", action="store_true")
    parser.add_argument("--headless", action="store_true")
    args = parser.parse_args()

    playwright, browser, context, page = create_browser(
        headless=args.headless
    )

    try:
        login_and_open_target(page)

        if args.keep_open:
            print("Browser will remain open. Press Ctrl+C to stop.")
            while True:
                time.sleep(5)

    finally:
        context.close()
        browser.close()
        playwright.stop()


if __name__ == "__main__":
    main()
