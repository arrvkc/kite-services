from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import os
from pathlib import Path
import re
from urllib.parse import urlsplit, urlunsplit

from dotenv import load_dotenv
from playwright.sync_api import sync_playwright


load_dotenv()

EAJEE_URL = os.getenv("EAJEE_URL", "https://eajee.in").rstrip("/")
EAJEE_USERNAME = os.getenv("EAJEE_USERNAME", "").strip()
EAJEE_PASSWORD = os.getenv("EAJEE_PASSWORD", "").strip()
EAJEE_PROFILE_DIR = Path(
    os.getenv("EAJEE_PROFILE_DIR", "server_runner/profile_eajee")
)

LOGIN_PATH = "/login"
AUTH_PROBE_PATH = "/settings"
UPLOAD_PATH = "/data/mu"
UPLOAD_SUCCESS_PATH = "/data/instrument_list"


class AuthState(str, Enum):
    AUTHENTICATED = "AUTHENTICATED"
    LOGIN_REQUIRED = "LOGIN_REQUIRED"
    UNKNOWN = "UNKNOWN"


@dataclass(frozen=True)
class AuthContract:
    path: str
    page_title: str
    identity_count: int
    password_count: int
    submit_count: int
    logout_count: int


@dataclass(frozen=True)
class UploadContract:
    path: str
    http_status: int | None
    page_title: str
    logout_count: int
    file_input_count: int
    import_control_count: int
    internal_server_error: bool


def safe_url_for_log(value: str) -> str:
    """Return only non-sensitive URL identity for operational diagnostics."""

    try:
        parsed = urlsplit(str(value or ""))
        if parsed.scheme or parsed.netloc:
            return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, "", ""))
        return parsed.path or "<unavailable-url>"
    except (TypeError, ValueError):
        return "<unavailable-url>"


def _url_path(value: str) -> str:
    try:
        return urlsplit(str(value or "")).path
    except (TypeError, ValueError):
        return ""


def _visible_count(locator) -> int:
    return sum(locator.nth(index).is_visible() for index in range(locator.count()))


def _auth_contract(page) -> AuthContract:
    return AuthContract(
        path=_url_path(page.url),
        page_title=page.title(),
        identity_count=_visible_count(page.locator('input[name="identity"]')),
        password_count=_visible_count(
            page.locator('input[name="password"][type="password"]')
        ),
        submit_count=_visible_count(
            page.locator('button[type="submit"][name="login"]')
        ),
        logout_count=page.locator(
            'a[href="/logout"], a[href$="/logout"]'
        ).count(),
    )


def classify_auth_contract(contract: AuthContract) -> AuthState:
    if contract.path == AUTH_PROBE_PATH and contract.logout_count > 0:
        return AuthState.AUTHENTICATED
    if (
        contract.path == LOGIN_PATH
        and contract.identity_count == 1
        and contract.password_count == 1
        and contract.submit_count == 1
    ):
        return AuthState.LOGIN_REQUIRED
    return AuthState.UNKNOWN


def _auth_diagnostic(contract: AuthContract) -> str:
    return (
        f"path={contract.path or '<unknown>'} "
        f"title={contract.page_title!r} "
        f"identity_count={contract.identity_count} "
        f"password_count={contract.password_count} "
        f"submit_count={contract.submit_count} "
        f"logout_count={contract.logout_count}"
    )


def discover_auth_state(page) -> tuple[AuthState, AuthContract]:
    page.goto(f"{EAJEE_URL}{LOGIN_PATH}", wait_until="domcontentloaded")
    contract = _auth_contract(page)
    return classify_auth_contract(contract), contract


def reset_login_required_session(page) -> AuthContract:
    """Remove conflicting Eajee cookies before a credential login attempt."""

    hostname = urlsplit(EAJEE_URL).hostname
    if not hostname:
        raise RuntimeError("EAJEE_URL has no valid hostname")
    page.context.clear_cookies(
        domain=re.compile(rf"^\.?{re.escape(hostname)}$")
    )
    page.goto(f"{EAJEE_URL}{LOGIN_PATH}", wait_until="domcontentloaded")
    contract = _auth_contract(page)
    if classify_auth_contract(contract) is not AuthState.LOGIN_REQUIRED:
        raise RuntimeError(
            "Eajee login form contract failed after session reset; "
            + _auth_diagnostic(contract)
        )
    return contract


def verify_authenticated_session(page) -> bool:
    page.goto(f"{EAJEE_URL}{AUTH_PROBE_PATH}", wait_until="domcontentloaded")
    return classify_auth_contract(_auth_contract(page)) is AuthState.AUTHENTICATED


def login_if_required(
    page,
    username: str = EAJEE_USERNAME,
    password: str = EAJEE_PASSWORD,
) -> str:
    state, contract = discover_auth_state(page)
    if state is AuthState.AUTHENTICATED:
        return "LOGIN_NOT_REQUIRED"
    if state is AuthState.UNKNOWN:
        raise RuntimeError(
            "Eajee authentication state is unknown; " + _auth_diagnostic(contract)
        )
    if not username or not password:
        raise RuntimeError("EAJEE_USERNAME / EAJEE_PASSWORD missing in environment")

    reset_login_required_session(page)

    identity_input = page.locator('input[name="identity"]')
    password_input = page.locator('input[name="password"][type="password"]')
    submit = page.locator('button[type="submit"][name="login"]')
    identity_input.fill(username)
    password_input.fill(password)
    with page.expect_navigation(wait_until="domcontentloaded"):
        submit.click()

    if not verify_authenticated_session(page):
        failed = _auth_contract(page)
        raise RuntimeError(
            "Eajee authentication failed; " + _auth_diagnostic(failed)
        )
    return "LOGIN_SUCCESS"


def _upload_contract(page, response) -> UploadContract:
    body_text = page.inner_text("body") if page.locator("body").count() else ""
    return UploadContract(
        path=_url_path(page.url),
        http_status=response.status if response is not None else None,
        page_title=page.title(),
        logout_count=page.locator(
            'a[href="/logout"], a[href$="/logout"]'
        ).count(),
        file_input_count=_visible_count(page.locator('input[type="file"]')),
        import_control_count=_visible_count(
            page.get_by_role("button", name="Import", exact=True)
        ),
        internal_server_error=(
            "Internal Server Error" in body_text
            or page.title() == "Internal Server Error"
        ),
    )


def validate_upload_contract(contract: UploadContract) -> None:
    if contract.internal_server_error:
        raise RuntimeError("Eajee returned Internal Server Error for the upload route")
    if contract.http_status != 200 or contract.path != UPLOAD_PATH:
        raise RuntimeError(
            "Eajee upload route is inaccessible; "
            f"path={contract.path or '<unknown>'} status={contract.http_status}"
        )
    if contract.logout_count == 0:
        raise RuntimeError("Eajee upload route lacks an authenticated shell")
    if contract.file_input_count != 1:
        raise RuntimeError(
            "Eajee upload route file input contract failed; "
            f"count={contract.file_input_count}"
        )
    if contract.import_control_count != 1:
        raise RuntimeError(
            "Eajee upload route Import control contract failed; "
            f"count={contract.import_control_count}"
        )


def open_upload_page(page) -> UploadContract:
    response = page.goto(f"{EAJEE_URL}{UPLOAD_PATH}", wait_until="domcontentloaded")
    contract = _upload_contract(page, response)
    validate_upload_contract(contract)
    return contract


def upload_succeeded(final_url: str) -> bool:
    return _url_path(final_url) == UPLOAD_SUCCESS_PATH


def latest_mojo_html() -> Path:
    files = list(Path("saved_pages").glob("marketsmojo_server_mojoscore_*.html"))
    if not files:
        raise FileNotFoundError(
            "No MarketsMojo server HTML file found in saved_pages/"
        )
    return sorted(files, key=lambda item: item.stat().st_mtime)[-1]


def upload_html(page, html_file: Path) -> str:
    open_upload_page(page)
    page.locator('input[type="file"]').set_input_files(str(html_file.resolve()))
    print("File selected.")
    page.get_by_role("button", name="Import", exact=True).click()

    for _ in range(120):
        if upload_succeeded(page.url):
            print("UPLOAD_SUCCESS")
            return safe_url_for_log(page.url)
        body_text = page.inner_text("body") if page.locator("body").count() else ""
        if "Internal Server Error" in body_text:
            Path("screenshots").mkdir(parents=True, exist_ok=True)
            page.screenshot(
                path="screenshots/eajee_upload_internal_server_error.png",
                full_page=True,
            )
            raise RuntimeError("Eajee returned Internal Server Error after import")
        page.wait_for_timeout(1000)

    Path("screenshots").mkdir(parents=True, exist_ok=True)
    page.screenshot(path="screenshots/eajee_upload_timeout.png", full_page=True)
    raise RuntimeError(
        "Upload did not reach the expected success route; "
        f"final_url={safe_url_for_log(page.url)}"
    )


def main() -> None:
    html_file = latest_mojo_html()
    print("Uploading:", html_file)

    with sync_playwright() as playwright:
        context = playwright.chromium.launch_persistent_context(
            user_data_dir=str(EAJEE_PROFILE_DIR),
            headless=False,
            channel="chrome",
            viewport={"width": 1400, "height": 900},
        )
        try:
            page = context.pages[0] if context.pages else context.new_page()
            login_status = login_if_required(page)
            print("LOGIN_STATUS=", login_status, sep="")
            print("AUTH_STATE_AFTER_LOGIN=AUTHENTICATED")
            print("DATA_MU_ACCESS=PASS")
            final_url = upload_html(page, html_file)
            print("UPLOADED_FILE=", html_file, sep="")
            print("FINAL_URL=", final_url, sep="")
            print("UPLOAD_RESULT=PASS")
        finally:
            context.close()


if __name__ == "__main__":
    main()
