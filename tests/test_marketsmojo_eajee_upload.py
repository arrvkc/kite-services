import importlib.util
from pathlib import Path
import sys
import unittest
from unittest.mock import MagicMock, patch


SOURCE = (
    Path(__file__).resolve().parents[1]
    / "marketsmojo_automation"
    / "server_runner"
    / "upload_mojo_html.py"
)
SPEC = importlib.util.spec_from_file_location("marketsmojo_eajee_upload", SOURCE)
upload = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = upload
SPEC.loader.exec_module(upload)


def auth_contract(
    path,
    *,
    identity=0,
    password=0,
    submit=0,
    logout=0,
):
    return upload.AuthContract(
        path=path,
        page_title="Synthetic Eajee",
        identity_count=identity,
        password_count=password,
        submit_count=submit,
        logout_count=logout,
    )


def upload_contract(
    *,
    path="/data/mu",
    status=200,
    logout=1,
    file_inputs=1,
    import_controls=1,
    internal_error=False,
):
    return upload.UploadContract(
        path=path,
        http_status=status,
        page_title="Mojo HTML upload",
        logout_count=logout,
        file_input_count=file_inputs,
        import_control_count=import_controls,
        internal_server_error=internal_error,
    )


class MarketsMojoEajeeUploadTests(unittest.TestCase):
    def test_already_authenticated_does_not_fill_credentials(self):
        page = MagicMock()
        evidence = auth_contract("/settings", logout=1)
        with patch.object(
            upload,
            "discover_auth_state",
            return_value=(upload.AuthState.AUTHENTICATED, evidence),
        ):
            result = upload.login_if_required(
                page, "synthetic-user", "synthetic-pass"
            )

        self.assertEqual(result, "LOGIN_NOT_REQUIRED")
        page.locator.assert_not_called()

    def test_current_canonical_login_form_is_used_and_verified(self):
        page = MagicMock()
        identity = MagicMock()
        password = MagicMock()
        submit = MagicMock()
        page.locator.side_effect = lambda selector: {
            'input[name="identity"]': identity,
            'input[name="password"][type="password"]': password,
            'button[type="submit"][name="login"]': submit,
        }[selector]
        evidence = auth_contract(
            "/login", identity=1, password=1, submit=1
        )
        with patch.object(
            upload,
            "discover_auth_state",
            return_value=(upload.AuthState.LOGIN_REQUIRED, evidence),
        ), patch.object(
            upload, "reset_login_required_session", return_value=evidence
        ), patch.object(upload, "verify_authenticated_session", return_value=True):
            result = upload.login_if_required(
                page, "synthetic-user", "synthetic-pass"
            )

        self.assertEqual(result, "LOGIN_SUCCESS")
        identity.fill.assert_called_once_with("synthetic-user")
        password.fill.assert_called_once_with("synthetic-pass")
        submit.click.assert_called_once_with()
        page.expect_navigation.assert_called_once_with(
            wait_until="domcontentloaded"
        )

    def test_login_required_resets_only_eajee_cookie_scope(self):
        page = MagicMock()
        evidence = auth_contract(
            "/login", identity=1, password=1, submit=1
        )
        with patch.object(upload, "_auth_contract", return_value=evidence):
            result = upload.reset_login_required_session(page)

        self.assertEqual(result, evidence)
        cookie_call = page.context.clear_cookies.call_args
        self.assertIsNotNone(cookie_call)
        domain_pattern = cookie_call.kwargs["domain"]
        self.assertIsNotNone(domain_pattern.fullmatch("eajee.in"))
        self.assertIsNotNone(domain_pattern.fullmatch(".eajee.in"))
        self.assertIsNone(domain_pattern.fullmatch("kite.zerodha.com"))
        page.goto.assert_called_once_with(
            "https://eajee.in/login", wait_until="domcontentloaded"
        )

    def test_session_reset_fails_closed_if_login_form_disappears(self):
        page = MagicMock()
        unknown = auth_contract("/login", identity=0, password=0, submit=0)
        with patch.object(upload, "_auth_contract", return_value=unknown):
            with self.assertRaisesRegex(
                RuntimeError, "login form contract failed after session reset"
            ):
                upload.reset_login_required_session(page)

    def test_login_required_but_form_missing_fails_immediately(self):
        page = MagicMock()
        evidence = auth_contract("/", identity=0, password=0, submit=0)
        with patch.object(
            upload,
            "discover_auth_state",
            return_value=(upload.AuthState.UNKNOWN, evidence),
        ):
            with self.assertRaisesRegex(RuntimeError, "state is unknown"):
                upload.login_if_required(
                    page, "synthetic-user", "synthetic-pass"
                )

        page.locator.assert_not_called()
        page.wait_for_timeout.assert_not_called()

    def test_failed_authentication_does_not_claim_success(self):
        page = MagicMock()
        identity = MagicMock()
        password = MagicMock()
        submit = MagicMock()
        page.locator.side_effect = lambda selector: {
            'input[name="identity"]': identity,
            'input[name="password"][type="password"]': password,
            'button[type="submit"][name="login"]': submit,
        }[selector]
        login = auth_contract("/login", identity=1, password=1, submit=1)
        failed = auth_contract("/login", identity=1, password=1, submit=1)
        with patch.object(
            upload,
            "discover_auth_state",
            return_value=(upload.AuthState.LOGIN_REQUIRED, login),
        ), patch.object(
            upload, "reset_login_required_session", return_value=login
        ), patch.object(
            upload, "verify_authenticated_session", return_value=False
        ), patch.object(upload, "_auth_contract", return_value=failed):
            with self.assertRaisesRegex(RuntimeError, "authentication failed"):
                upload.login_if_required(
                    page, "synthetic-user", "synthetic-pass"
                )

    def test_upload_route_inaccessible_after_auth_fails_closed(self):
        with self.assertRaisesRegex(RuntimeError, "route is inaccessible"):
            upload.validate_upload_contract(
                upload_contract(path="/login", status=200, logout=0)
            )

    def test_upload_route_without_file_input_fails_closed(self):
        with self.assertRaisesRegex(RuntimeError, "file input contract failed"):
            upload.validate_upload_contract(upload_contract(file_inputs=0))

    def test_upload_route_without_import_control_fails_closed(self):
        with self.assertRaisesRegex(RuntimeError, "Import control contract failed"):
            upload.validate_upload_contract(upload_contract(import_controls=0))

    def test_success_requires_exact_known_application_route(self):
        self.assertTrue(
            upload.upload_succeeded(
                "https://eajee.in/data/instrument_list?source=synthetic"
            )
        )
        self.assertFalse(upload.upload_succeeded("https://eajee.in/data/mu"))
        self.assertFalse(
            upload.upload_succeeded(
                "https://eajee.in/data/instrument_list/other"
            )
        )

    def test_auth_classifier_requires_complete_positive_contract(self):
        self.assertEqual(
            upload.classify_auth_contract(
                auth_contract("/login", identity=1, password=1, submit=1)
            ),
            upload.AuthState.LOGIN_REQUIRED,
        )
        self.assertEqual(
            upload.classify_auth_contract(
                auth_contract("/settings", logout=1)
            ),
            upload.AuthState.AUTHENTICATED,
        )
        self.assertEqual(
            upload.classify_auth_contract(
                auth_contract("/login", identity=1, password=1, submit=0)
            ),
            upload.AuthState.UNKNOWN,
        )

    def test_safe_url_diagnostics_remove_query_and_fragment(self):
        result = upload.safe_url_for_log(
            "https://eajee.in/login?next=/data/mu#transient"
        )
        self.assertEqual(result, "https://eajee.in/login")
        self.assertNotIn("next", result)
        self.assertNotIn("transient", result)


if __name__ == "__main__":
    unittest.main()
