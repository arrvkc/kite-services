import unittest

from automations.eajee_web_automation.persisted_refresh_verification import (
    DONE,
    SUCCESS,
    UNKNOWN_FINAL_STATE,
    locate_target_user_state,
    parse_updated_on,
    verify_persisted_refresh,
)


HEADERS = [
    "ID",
    "Username",
    "Zerodha User ID",
    "Kite API Subscription",
    "Updated On",
    "Refresh",
]


class FakeCell:
    def __init__(self, text):
        self._text = text

    def inner_text(self):
        return self._text


class FakeCollection:
    def __init__(self, items):
        self._items = items

    def count(self):
        return len(self._items)

    def nth(self, index):
        return self._items[index]


class FakeRow:
    def __init__(self, values):
        self.cells = FakeCollection([FakeCell(value) for value in values])

    def locator(self, selector):
        if selector != "td":
            raise AssertionError(f"Unexpected row selector: {selector}")
        return self.cells


class FakeTable:
    def __init__(self, rows):
        self.headers = FakeCollection([FakeCell(value) for value in HEADERS])
        self.rows = FakeCollection([FakeRow(row) for row in rows])

    def count(self):
        return 1

    def locator(self, selector):
        if selector == "thead th":
            return self.headers
        if selector == "tbody tr":
            return self.rows
        raise AssertionError(f"Unexpected table selector: {selector}")


class FakePage:
    def __init__(
        self,
        rows,
        url="https://eajee.in/data/users",
        body_text="",
    ):
        self.url = url
        self.table = FakeTable(rows)
        self.body = FakeCell(body_text)

    def locator(self, selector):
        if selector == "#users-table":
            return self.table
        if selector == "body":
            return self.body
        raise AssertionError(f"Unexpected page selector: {selector}")


def row(user_id, updated_on, row_id="1"):
    return [row_id, "user", user_id, "Yes", updated_on, "refresh"]


class PersistedRefreshVerificationTests(unittest.TestCase):
    def test_timestamp_advances_without_toast_and_finishes_done(self):
        before = parse_updated_on("2026-08-20 08:15:19 IST")
        page = FakePage([row("TARGET", "2026-08-20 08:20:01 IST")])

        result = verify_persisted_refresh(page, "TARGET", before)

        self.assertEqual(result.status, SUCCESS)
        self.assertEqual(result.stage, DONE)
        self.assertEqual(result.reason, "")

    def test_unchanged_timestamp_is_not_success(self):
        before = parse_updated_on("2026-08-20 08:15:19 IST")
        page = FakePage([row("TARGET", "2026-08-20 08:15:19 IST")])

        result = verify_persisted_refresh(page, "TARGET", before)

        self.assertEqual(result.status, UNKNOWN_FINAL_STATE)
        self.assertIn("did not advance", result.reason)

    def test_generic_success_text_cannot_create_false_success(self):
        before = parse_updated_on("2026-08-20 08:15:19 IST")
        page = FakePage(
            [row("TARGET", "2026-08-20 08:15:19 IST")],
            body_text="Success",
        )

        result = verify_persisted_refresh(page, "TARGET", before)

        self.assertEqual(page.locator("body").inner_text(), "Success")
        self.assertNotEqual(result.status, SUCCESS)

    def test_target_user_missing_is_diagnostic_and_not_success(self):
        before = parse_updated_on("2026-08-20 08:15:19 IST")
        page = FakePage([row("OTHER", "2026-08-20 08:20:01 IST")])

        result = verify_persisted_refresh(page, "TARGET", before)

        self.assertEqual(result.status, UNKNOWN_FINAL_STATE)
        self.assertIn("Target Zerodha user row was not found", result.reason)

    def test_wrong_users_timestamp_change_cannot_create_success(self):
        before = parse_updated_on("2026-08-20 08:15:19 IST")
        page = FakePage(
            [
                row("OTHER", "2026-08-20 08:20:01 IST", "1"),
                row("TARGET", "2026-08-20 08:15:19 IST", "2"),
            ]
        )

        result = verify_persisted_refresh(page, "TARGET", before)

        self.assertEqual(result.status, UNKNOWN_FINAL_STATE)
        self.assertIn("did not advance", result.reason)

    def test_current_ist_timestamp_format_is_parsed_strictly(self):
        parsed = parse_updated_on("2026-08-20 08:15:19 IST")

        self.assertEqual(parsed.strftime("%Y-%m-%d %H:%M:%S %Z"), "2026-08-20 08:15:19 IST")
        with self.assertRaisesRegex(Exception, "Updated On is malformed"):
            parse_updated_on("2026/08/20 08:15:19 IST")

    def test_target_row_is_selected_by_exact_zerodha_user_id(self):
        page = FakePage(
            [
                row("TARGET-SUFFIX", "2026-08-20 08:30:01 IST", "1"),
                row("TARGET", "2026-08-20 08:20:01 IST", "2"),
            ]
        )

        state = locate_target_user_state(page, "TARGET")

        self.assertEqual(state.updated_on, parse_updated_on("2026-08-20 08:20:01 IST"))

    def test_wrong_application_state_is_not_success(self):
        before = parse_updated_on("2026-08-20 08:15:19 IST")
        page = FakePage(
            [row("TARGET", "2026-08-20 08:20:01 IST")],
            url="https://kite.zerodha.com/connect/login",
        )

        result = verify_persisted_refresh(page, "TARGET", before)

        self.assertEqual(result.status, UNKNOWN_FINAL_STATE)
        self.assertIn("Expected EAJEE users page", result.reason)

    def test_malformed_post_refresh_timestamp_is_not_success(self):
        before = parse_updated_on("2026-08-20 08:15:19 IST")
        page = FakePage([row("TARGET", "20-08-2026 08:20:01 IST")])

        result = verify_persisted_refresh(page, "TARGET", before)

        self.assertEqual(result.status, UNKNOWN_FINAL_STATE)
        self.assertIn("Updated On is malformed", result.reason)

    def test_timestamp_moving_backwards_is_not_success(self):
        before = parse_updated_on("2026-08-20 08:15:19 IST")
        page = FakePage([row("TARGET", "2026-08-20 08:14:59 IST")])

        result = verify_persisted_refresh(page, "TARGET", before)

        self.assertEqual(result.status, UNKNOWN_FINAL_STATE)
        self.assertIn("moved backwards", result.reason)


if __name__ == "__main__":
    unittest.main()
