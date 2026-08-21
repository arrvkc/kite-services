import unittest
from pathlib import Path

from automations.eajee_web_automation.diagnostic_urls import (
    safe_diagnostic_text,
    safe_url_for_log,
)


class DiagnosticUrlSanitizationTests(unittest.TestCase):
    def test_absolute_url_discards_complete_query(self):
        result = safe_url_for_log(
            "https://kite.zerodha.com/connect/login?api_key=test&sess_id=secret"
        )

        self.assertEqual(result, "https://kite.zerodha.com/connect/login")
        for forbidden in ("api_key", "sess_id", "test", "secret"):
            self.assertNotIn(forbidden, result)

    def test_query_and_fragment_are_discarded(self):
        self.assertEqual(
            safe_url_for_log("https://example.test/path?x=1#sensitive"),
            "https://example.test/path",
        )

    def test_clean_internal_url_is_unchanged(self):
        self.assertEqual(
            safe_url_for_log("https://eajee.in/data/users"),
            "https://eajee.in/data/users",
        )

    def test_relative_url_discards_query(self):
        self.assertEqual(
            safe_url_for_log("/connect/start/3?temporary=value"),
            "/connect/start/3",
        )

    def test_embedded_urls_in_failure_diagnostics_are_sanitized(self):
        result = safe_diagnostic_text(
            "navigation failed at "
            "https://kite.zerodha.com/connect/login?api_key=synthetic&state=temporary"
        )

        self.assertEqual(
            result,
            "navigation failed at https://kite.zerodha.com/connect/login",
        )
        self.assertNotIn("synthetic", result)
        self.assertNotIn("temporary", result)

    def test_unusual_url_never_raises_or_reproduces_input(self):
        unsafe = "https://[malformed?secret=value"

        result = safe_url_for_log(unsafe)

        self.assertEqual(result, "<unavailable-url>")
        self.assertNotIn("secret", result)

    def test_token_refresh_diagnostics_use_canonical_sanitizer(self):
        package = (
            Path(__file__).resolve().parents[1]
            / "automations"
            / "eajee_web_automation"
        )
        login_source = (package / "login.py").read_text(encoding="utf-8")
        zerodha_source = (package / "zerodha_login.py").read_text(encoding="utf-8")
        refresh_source = (package / "refresh_user_token.py").read_text(
            encoding="utf-8"
        )

        self.assertNotIn('print("Current URL:", page.url)', login_source)
        self.assertNotIn('print("Zerodha page URL:", page.url)', zerodha_source)
        self.assertNotIn('print("After Zerodha login URL:", page.url)', zerodha_source)
        self.assertNotIn('print(f"After refresh click URL: {final_url}")', refresh_source)
        self.assertNotIn('print(f"FINAL_URL={page.url}")', refresh_source)
        self.assertIn("safe_url_for_log", login_source)
        self.assertIn("safe_url_for_log", zerodha_source)
        self.assertIn('safe_row["final_url"] = safe_url_for_log', refresh_source)
        self.assertIn("safe_diagnostic_text(exc)", refresh_source)


if __name__ == "__main__":
    unittest.main()
