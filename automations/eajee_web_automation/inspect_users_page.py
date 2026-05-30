from .browser import create_browser
from .login import login_and_open_target


def main():
    playwright, browser, context, page = create_browser(headless=False)

    try:
        login_and_open_target(page)

        print("\n===== CURRENT URL =====")
        print(page.url)

        print("\n===== PAGE TITLE =====")
        print(page.title())

        print("\n===== INPUTS =====")
        inputs = page.locator("input")
        for i in range(inputs.count()):
            print(inputs.nth(i).evaluate("""
                el => ({
                    tag: el.tagName,
                    type: el.type,
                    id: el.id,
                    name: el.name,
                    placeholder: el.placeholder,
                    value: el.value,
                    className: el.className
                })
            """))

        print("\n===== BUTTONS =====")
        buttons = page.locator("button")
        for i in range(buttons.count()):
            print(buttons.nth(i).evaluate("""
                el => ({
                    text: el.innerText,
                    id: el.id,
                    type: el.type,
                    className: el.className,
                    title: el.title
                })
            """))

        print("\n===== LINKS =====")
        links = page.locator("a")
        for i in range(links.count()):
            data = links.nth(i).evaluate("""
                el => ({
                    text: el.innerText,
                    href: el.href,
                    id: el.id,
                    className: el.className,
                    title: el.title
                })
            """)
            if "XJ1877" in str(data) or "refresh" in str(data).lower() or "sync" in str(data).lower():
                print(data)

        print("\n===== ROWS CONTAINING XJ1877 =====")
        rows = page.locator("tr")
        for i in range(rows.count()):
            text = rows.nth(i).inner_text()
            if "XJ1877" in text:
                print(text)
                print(rows.nth(i).evaluate("el => el.outerHTML"))

    finally:
        context.close()
        browser.close()
        playwright.stop()


if __name__ == "__main__":
    main()
