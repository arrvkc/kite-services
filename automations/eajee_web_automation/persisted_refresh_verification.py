"""Deterministic persisted-state verification for EAJEE token refreshes."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo


SUCCESS = "SUCCESS"
UNKNOWN_FINAL_STATE = "UNKNOWN_FINAL_STATE"
VERIFY_FINAL_RESULT = "VERIFY_FINAL_RESULT"
DONE = "DONE"
USERS_PATH = "/data/users"
UPDATED_ON_FORMAT = "%Y-%m-%d %H:%M:%S IST"
IST = ZoneInfo("Asia/Kolkata")


class PersistedStateVerificationError(RuntimeError):
    """The displayed persisted state cannot be verified safely."""


@dataclass(frozen=True)
class TargetUserState:
    row: Any
    updated_on: datetime


@dataclass(frozen=True)
class FinalVerification:
    status: str
    stage: str
    reason: str
    post_refresh_updated_on: datetime | None = None


def parse_updated_on(value: str) -> datetime:
    """Parse the exact timestamp format rendered by the EAJEE users table."""
    normalized = str(value).strip()
    try:
        parsed = datetime.strptime(normalized, UPDATED_ON_FORMAT)
    except ValueError as exc:
        raise PersistedStateVerificationError(
            "Updated On is malformed; expected YYYY-MM-DD HH:MM:SS IST"
        ) from exc
    return parsed.replace(tzinfo=IST)


def _column_index(headers: list[str], expected: str) -> int:
    matches = [
        index
        for index, header in enumerate(headers)
        if header.strip().casefold() == expected.casefold()
    ]
    if len(matches) != 1:
        raise PersistedStateVerificationError(
            f"Expected exactly one {expected!r} table column"
        )
    return matches[0]


def locate_target_user_state(page, target_user_id: str) -> TargetUserState:
    """Locate one exact Zerodha user row and return its displayed persisted state."""
    table = page.locator("#users-table")
    if table.count() != 1:
        raise PersistedStateVerificationError("EAJEE users table was not found")

    header_cells = table.locator("thead th")
    headers = [
        header_cells.nth(index).inner_text().strip()
        for index in range(header_cells.count())
    ]
    user_id_index = _column_index(headers, "Zerodha User ID")
    updated_on_index = _column_index(headers, "Updated On")
    required_cell_count = max(user_id_index, updated_on_index) + 1

    matching_rows = []
    rows = table.locator("tbody tr")
    for index in range(rows.count()):
        row = rows.nth(index)
        cells = row.locator("td")
        if cells.count() < required_cell_count:
            continue
        displayed_user_id = cells.nth(user_id_index).inner_text().strip()
        if displayed_user_id == target_user_id:
            matching_rows.append((row, cells))

    if not matching_rows:
        raise PersistedStateVerificationError(
            f"Target Zerodha user row was not found: {target_user_id}"
        )
    if len(matching_rows) != 1:
        raise PersistedStateVerificationError(
            f"Multiple rows matched target Zerodha user ID: {target_user_id}"
        )

    row, cells = matching_rows[0]
    updated_on = parse_updated_on(cells.nth(updated_on_index).inner_text())
    return TargetUserState(row=row, updated_on=updated_on)


def verify_persisted_refresh(
    page,
    target_user_id: str,
    pre_refresh_updated_on: datetime,
) -> FinalVerification:
    """Verify success only when the exact target user's persisted timestamp advances."""
    final_path = urlsplit(page.url).path.rstrip("/") or "/"
    if final_path != USERS_PATH:
        return FinalVerification(
            status=UNKNOWN_FINAL_STATE,
            stage=VERIFY_FINAL_RESULT,
            reason=(
                "Expected EAJEE users page was not reached; "
                f"current path is {final_path}"
            ),
        )

    try:
        post_state = locate_target_user_state(page, target_user_id)
    except PersistedStateVerificationError as exc:
        return FinalVerification(
            status=UNKNOWN_FINAL_STATE,
            stage=VERIFY_FINAL_RESULT,
            reason=str(exc),
        )

    post_refresh_updated_on = post_state.updated_on
    if post_refresh_updated_on == pre_refresh_updated_on:
        return FinalVerification(
            status=UNKNOWN_FINAL_STATE,
            stage=VERIFY_FINAL_RESULT,
            reason=f"Updated On did not advance for {target_user_id}",
            post_refresh_updated_on=post_refresh_updated_on,
        )
    if post_refresh_updated_on < pre_refresh_updated_on:
        return FinalVerification(
            status=UNKNOWN_FINAL_STATE,
            stage=VERIFY_FINAL_RESULT,
            reason=f"Updated On moved backwards for {target_user_id}",
            post_refresh_updated_on=post_refresh_updated_on,
        )

    return FinalVerification(
        status=SUCCESS,
        stage=DONE,
        reason="",
        post_refresh_updated_on=post_refresh_updated_on,
    )
