"""Authoritative NSE cash-market session classification.

Current-day state and declared holidays come from official NSE JSON endpoints.
Validated UDiFF final archives are the conclusive historical session record.
No clock time or locally maintained holiday list is treated as authority.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from enum import Enum
import hashlib
import json
from zoneinfo import ZoneInfo

import requests

from services.nse_final_close import (
    NseFinalCloseNotPublished,
    NseFinalCloseProvider,
    NseFinalCloseUnavailable,
)


IST_NAME = "Asia/Kolkata"
NSE_HOME = "https://www.nseindia.com/"
NSE_HOLIDAY_URL = "https://www.nseindia.com/api/holiday-master?type=trading"
NSE_MARKET_STATUS_URL = "https://www.nseindia.com/api/marketStatus"
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json,text/plain,*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/resources/exchange-communication-holidays",
}


class SessionClassification(str, Enum):
    NO_TRADING_SESSION = "NO_TRADING_SESSION"
    NORMAL_SESSION = "NORMAL_SESSION"
    SPECIAL_SESSION = "SPECIAL_SESSION"
    SHORTENED_SESSION = "SHORTENED_SESSION"
    UNRESOLVED = "UNRESOLVED"


class SessionReadiness(str, Enum):
    NO_SESSION = "NO_SESSION"
    NOT_COMPLETE = "NOT_COMPLETE"
    COMPLETE = "COMPLETE"
    UNRESOLVED = "UNRESOLVED"


@dataclass(frozen=True)
class NseTradingSessionEvidence:
    session_date: date
    segment: str
    classification: SessionClassification
    readiness: SessionReadiness
    session_start: datetime | None
    session_end: datetime | None
    source: str
    provenance: str
    checked_at: datetime
    reason_code: str


class NseTradingSessionUnavailable(RuntimeError):
    """Official NSE session metadata is unavailable or invalid."""


def _canonical_digest(payload: object) -> str:
    content = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode("ascii")
    return hashlib.sha256(content).hexdigest()


def _parse_nse_datetime(value: object) -> datetime | None:
    text = str(value or "").strip()
    for pattern in ("%d-%b-%Y %H:%M", "%d-%b-%Y"):
        try:
            parsed = datetime.strptime(text, pattern)
        except ValueError:
            continue
        return parsed.replace(tzinfo=ZoneInfo(IST_NAME))
    return None


class NseTradingSessionProvider:
    """Resolve CM session existence/completion from official NSE evidence."""

    def __init__(
        self,
        session: requests.Session | None = None,
        timeout: int = 30,
        final_close_provider: NseFinalCloseProvider | None = None,
    ) -> None:
        self._session = session or requests.Session()
        self._session.headers.update(_HEADERS)
        self._timeout = timeout
        self._final_close_provider = final_close_provider or NseFinalCloseProvider(
            session=self._session,
            timeout=timeout,
        )
        self._seeded = False

    def _json(self, url: str) -> tuple[dict, str]:
        try:
            if not self._seeded:
                self._session.get(NSE_HOME, timeout=self._timeout)
                self._seeded = True
            response = self._session.get(url, timeout=self._timeout)
            response.raise_for_status()
            payload = response.json()
        except (requests.RequestException, ValueError) as exc:
            raise NseTradingSessionUnavailable(
                "Official NSE session metadata is unavailable."
            ) from exc
        if not isinstance(payload, dict):
            raise NseTradingSessionUnavailable(
                "Official NSE session metadata has an invalid schema."
            )
        return payload, _canonical_digest(payload)

    @staticmethod
    def _holiday_dates(payload: dict) -> set[date]:
        rows = payload.get("CM")
        if not isinstance(rows, list):
            raise NseTradingSessionUnavailable(
                "Official NSE CM holiday metadata is unavailable."
            )
        result: set[date] = set()
        for row in rows:
            if not isinstance(row, dict):
                raise NseTradingSessionUnavailable(
                    "Official NSE CM holiday metadata has an invalid row."
                )
            try:
                parsed = datetime.strptime(
                    str(row.get("tradingDate") or ""), "%d-%b-%Y"
                ).date()
            except ValueError as exc:
                raise NseTradingSessionUnavailable(
                    "Official NSE CM holiday metadata has an invalid date."
                ) from exc
            if parsed in result:
                raise NseTradingSessionUnavailable(
                    "Official NSE CM holiday metadata contains a duplicate date."
                )
            result.add(parsed)
        return result

    @staticmethod
    def _capital_market_state(payload: dict) -> dict:
        rows = payload.get("marketState")
        if not isinstance(rows, list):
            raise NseTradingSessionUnavailable(
                "Official NSE market status metadata is unavailable."
            )
        matches = [
            row
            for row in rows
            if isinstance(row, dict)
            and str(row.get("market") or "").strip().upper() == "CAPITAL MARKET"
        ]
        if len(matches) != 1:
            raise NseTradingSessionUnavailable(
                "Official NSE Capital Market status is missing or ambiguous."
            )
        return matches[0]

    @staticmethod
    def _classification(target_date: date, holidays: set[date]):
        if target_date.weekday() >= 5 or target_date in holidays:
            return SessionClassification.SPECIAL_SESSION
        return SessionClassification.NORMAL_SESSION

    def previous_session(
        self, before_date: date, *, now: datetime | None = None
    ) -> NseTradingSessionEvidence:
        """Resolve the nearest prior session; skip only proven closures.

        Missing final data on an expected session is NOT a holiday. Return
        that pending/unknown evidence rather than silently choosing older data.
        The 31-day bound limits official requests, not the trading calendar.
        """
        for offset in range(1, 32):
            evidence = self.resolve(before_date - timedelta(days=offset), now=now)
            if evidence.readiness != SessionReadiness.NO_SESSION:
                return evidence
        return NseTradingSessionEvidence(
            before_date - timedelta(days=31), "CM",
            SessionClassification.UNRESOLVED, SessionReadiness.UNRESOLVED,
            None, None, evidence.source, evidence.provenance,
            evidence.checked_at, "PREVIOUS_SESSION_SEARCH_EXHAUSTED",
        )

    def resolve(
        self,
        target_date: date,
        *,
        now: datetime | None = None,
    ) -> NseTradingSessionEvidence:
        checked_at = now or datetime.now(timezone.utc)
        if checked_at.tzinfo is None:
            raise ValueError("Session-resolution time must be timezone-aware.")
        local_today = checked_at.astimezone(ZoneInfo(IST_NAME)).date()
        if target_date > local_today:
            return NseTradingSessionEvidence(
                target_date,
                "CM",
                SessionClassification.UNRESOLVED,
                SessionReadiness.UNRESOLVED,
                None,
                None,
                "NSE_OFFICIAL_SESSION_METADATA",
                "-",
                checked_at,
                "FUTURE_SESSION_DATE",
            )

        try:
            holidays_payload, holidays_hash = self._json(NSE_HOLIDAY_URL)
            market_payload, market_hash = self._json(NSE_MARKET_STATUS_URL)
            holidays = self._holiday_dates(holidays_payload)
            market = self._capital_market_state(market_payload)
        except NseTradingSessionUnavailable:
            return NseTradingSessionEvidence(
                target_date,
                "CM",
                SessionClassification.UNRESOLVED,
                SessionReadiness.UNRESOLVED,
                None,
                None,
                "NSE_OFFICIAL_SESSION_METADATA",
                "-",
                checked_at,
                "SESSION_METADATA_UNAVAILABLE",
            )

        classification = self._classification(target_date, holidays)
        market_timestamp = _parse_nse_datetime(market.get("tradeDate"))
        market_date = market_timestamp.date() if market_timestamp else None
        market_status = str(market.get("marketStatus") or "").strip().upper()
        provenance = hashlib.sha256(
            f"{holidays_hash}|{market_hash}".encode("ascii")
        ).hexdigest()

        def no_session_evidence(source: str) -> NseTradingSessionEvidence:
            return NseTradingSessionEvidence(
                target_date,
                "CM",
                SessionClassification.NO_TRADING_SESSION,
                SessionReadiness.NO_SESSION,
                None,
                None,
                source,
                provenance,
                checked_at,
                "NO_OFFICIAL_TRADING_SESSION",
            )

        if market_date == target_date:
            if market_status in {"CLOSE", "CLOSED"}:
                return NseTradingSessionEvidence(
                    target_date,
                    "CM",
                    classification,
                    SessionReadiness.COMPLETE,
                    None,
                    market_timestamp,
                    "NSE_HOLIDAY_MASTER+NSE_MARKET_STATUS",
                    provenance,
                    checked_at,
                    "OFFICIAL_MARKET_CLOSED",
                )
            if market_status in {"OPEN", "PRE-OPEN", "PREOPEN"}:
                return NseTradingSessionEvidence(
                    target_date,
                    "CM",
                    classification,
                    SessionReadiness.NOT_COMPLETE,
                    None,
                    None,
                    "NSE_HOLIDAY_MASTER+NSE_MARKET_STATUS",
                    provenance,
                    checked_at,
                    "OFFICIAL_MARKET_NOT_CLOSED",
                )
            return NseTradingSessionEvidence(
                target_date,
                "CM",
                SessionClassification.UNRESOLVED,
                SessionReadiness.UNRESOLVED,
                None,
                None,
                "NSE_HOLIDAY_MASTER+NSE_MARKET_STATUS",
                provenance,
                checked_at,
                "UNKNOWN_MARKET_STATUS",
            )

        try:
            catalog = self._final_close_provider.fetch_cash(target_date)
        except NseFinalCloseNotPublished:
            if target_date.weekday() >= 5 or target_date in holidays:
                return no_session_evidence(
                    "NSE_HOLIDAY_MASTER+NSE_FINAL_ARCHIVE"
                )
            return NseTradingSessionEvidence(
                target_date,
                "CM",
                SessionClassification.NORMAL_SESSION,
                SessionReadiness.NOT_COMPLETE,
                None,
                None,
                "NSE_HOLIDAY_MASTER+NSE_MARKET_STATUS",
                provenance,
                checked_at,
                "EXPECTED_SESSION_NOT_FINAL",
            )
        except NseFinalCloseUnavailable:
            # Valid current-day metadata can close an ordinary weekend without
            # treating ambiguous historical weekends or holidays as closed.
            current_expected_closure = (
                target_date == local_today
                and (target_date.weekday() >= 5 or target_date in holidays)
                and market_timestamp is not None
                and market_date is not None
                and market_date < target_date
                and market_status in {"CLOSE", "CLOSED"}
            )
            if current_expected_closure:
                return no_session_evidence(
                    "NSE_HOLIDAY_MASTER+NSE_MARKET_STATUS"
                )
            return NseTradingSessionEvidence(
                target_date,
                "CM",
                SessionClassification.UNRESOLVED,
                SessionReadiness.UNRESOLVED,
                None,
                None,
                "NSE_HOLIDAY_MASTER+NSE_FINAL_ARCHIVE",
                provenance,
                checked_at,
                "FINAL_ARCHIVE_UNAVAILABLE",
            )

        archive_provenance = hashlib.sha256(
            f"{provenance}|{catalog.source_sha256}".encode("ascii")
        ).hexdigest()
        return NseTradingSessionEvidence(
            target_date,
            "CM",
            classification,
            SessionReadiness.COMPLETE,
            None,
            None,
            "NSE_HOLIDAY_MASTER+NSE_FINAL_ARCHIVE",
            archive_provenance,
            checked_at,
            "OFFICIAL_FINAL_ARCHIVE_AVAILABLE",
        )
