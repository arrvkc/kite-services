from datetime import date, datetime, timezone
from unittest.mock import MagicMock

import requests

from services.nse_final_close import (
    NseFinalCloseCatalog,
    NseFinalCloseNotPublished,
    NseFinalCloseUnavailable,
)
from services.nse_trading_session import (
    NSE_HOLIDAY_URL,
    NSE_MARKET_STATUS_URL,
    NseTradingSessionProvider,
    SessionClassification,
    SessionReadiness,
)


NOW = datetime(2026, 9, 19, 12, tzinfo=timezone.utc)


def response(payload):
    result = MagicMock()
    result.json.return_value = payload
    result.raise_for_status.return_value = None
    return result


def provider(holidays, market_state, final=None):
    session = MagicMock()

    def get(url, **_kwargs):
        if url == NSE_HOLIDAY_URL:
            return response({"CM": holidays})
        if url == NSE_MARKET_STATUS_URL:
            return response({"marketState": [market_state]})
        return response({})

    session.get.side_effect = get
    final_provider = MagicMock()
    if isinstance(final, Exception):
        final_provider.fetch_cash.side_effect = final
    elif final is not None:
        final_provider.fetch_cash.return_value = final
    return NseTradingSessionProvider(
        session=session,
        final_close_provider=final_provider,
    )


def market(trade_date, status):
    return {
        "market": "Capital Market",
        "marketStatus": status,
        "tradeDate": trade_date,
        "marketStatusMessage": f"Market is {status}",
    }


def final_catalog(target):
    return NseFinalCloseCatalog(
        session_date=target,
        rows_by_segment={"CM": [], "FO": []},
        archive_hashes={"CM": "a" * 64, "FO": "b" * 64},
        archive_names={"CM": "cm.zip", "FO": "fo.zip"},
        retrieved_at=NOW,
    )


def test_normal_weekday_waits_for_official_close_then_completes():
    target = date(2026, 9, 18)
    active = provider([], market("18-Sep-2026 14:00", "Open")).resolve(
        target, now=NOW
    )
    closed = provider([], market("18-Sep-2026 15:30", "Close")).resolve(
        target, now=NOW
    )
    assert active.classification == SessionClassification.NORMAL_SESSION
    assert active.readiness == SessionReadiness.NOT_COMPLETE
    assert closed.readiness == SessionReadiness.COMPLETE
    assert closed.session_end is not None


def test_weekday_holiday_is_clean_no_session_when_final_archive_is_unavailable():
    holiday = date(2026, 9, 14)
    holiday_row = {"tradingDate": "14-Sep-2026"}
    prior = market("11-Sep-2026 15:30", "Close")
    holiday_evidence = provider(
        [holiday_row], prior, NseFinalCloseUnavailable("unavailable")
    ).resolve(
        holiday,
        now=datetime(2026, 9, 14, 12, tzinfo=timezone.utc),
    )
    assert (
        holiday_evidence.classification
        == SessionClassification.NO_TRADING_SESSION
    )
    assert holiday_evidence.readiness == SessionReadiness.NO_SESSION


def test_current_ordinary_sunday_without_final_archive_is_clean_no_session():
    target = date(2026, 9, 20)
    evidence = provider(
        [],
        market("18-Sep-2026 15:30", "Close"),
        NseFinalCloseUnavailable("unavailable"),
    ).resolve(target, now=datetime(2026, 9, 20, 12, tzinfo=timezone.utc))
    assert evidence.classification == SessionClassification.NO_TRADING_SESSION
    assert evidence.readiness == SessionReadiness.NO_SESSION


def test_current_ordinary_saturday_without_final_archive_is_clean_no_session():
    target = date(2026, 9, 19)
    evidence = provider(
        [],
        market("18-Sep-2026 15:30", "Close"),
        NseFinalCloseUnavailable("unavailable"),
    ).resolve(target, now=NOW)
    assert evidence.classification == SessionClassification.NO_TRADING_SESSION
    assert evidence.readiness == SessionReadiness.NO_SESSION


def test_explicitly_unpublished_historical_weekend_is_clean_no_session():
    target = date(2026, 9, 12)
    evidence = provider(
        [],
        market("18-Sep-2026 15:30", "Close"),
        NseFinalCloseNotPublished("missing"),
    ).resolve(target, now=NOW)
    assert evidence.classification == SessionClassification.NO_TRADING_SESSION
    assert evidence.readiness == SessionReadiness.NO_SESSION


def test_official_weekend_special_session_is_eligible_only_after_close():
    target = date(2026, 9, 19)
    active = provider([], market("19-Sep-2026 10:30", "Open")).resolve(
        target, now=NOW
    )
    closed = provider([], market("19-Sep-2026 12:30", "Close")).resolve(
        target, now=NOW
    )
    assert active.classification == SessionClassification.SPECIAL_SESSION
    assert active.readiness == SessionReadiness.NOT_COMPLETE
    assert closed.classification == SessionClassification.SPECIAL_SESSION
    assert closed.readiness == SessionReadiness.COMPLETE


def test_historical_special_session_is_proven_by_final_archive():
    target = date(2026, 9, 12)
    evidence = provider(
        [], market("18-Sep-2026 15:30", "Close"), final_catalog(target)
    ).resolve(target, now=NOW)
    assert evidence.classification == SessionClassification.SPECIAL_SESSION
    assert evidence.readiness == SessionReadiness.COMPLETE
    assert evidence.source.endswith("NSE_FINAL_ARCHIVE")


def test_historical_weekend_archive_transport_ambiguity_fails_closed():
    target = date(2026, 9, 12)
    evidence = provider(
        [],
        market("18-Sep-2026 15:30", "Close"),
        NseFinalCloseUnavailable("transport failure"),
    ).resolve(target, now=NOW)
    assert evidence.classification == SessionClassification.UNRESOLVED
    assert evidence.readiness == SessionReadiness.UNRESOLVED
    assert evidence.reason_code == "FINAL_ARCHIVE_UNAVAILABLE"


def test_historical_holiday_archive_transport_ambiguity_fails_closed():
    target = date(2026, 9, 14)
    evidence = provider(
        [{"tradingDate": "14-Sep-2026"}],
        market("18-Sep-2026 15:30", "Close"),
        NseFinalCloseUnavailable("transport failure"),
    ).resolve(target, now=NOW)
    assert evidence.classification == SessionClassification.UNRESOLVED
    assert evidence.readiness == SessionReadiness.UNRESOLVED
    assert evidence.reason_code == "FINAL_ARCHIVE_UNAVAILABLE"


def test_current_holiday_market_status_proves_special_session():
    target = date(2026, 9, 14)
    evidence = provider(
        [{"tradingDate": "14-Sep-2026"}],
        market("14-Sep-2026 18:00", "Open"),
        NseFinalCloseUnavailable("unavailable"),
    ).resolve(
        target,
        now=datetime(2026, 9, 14, 12, tzinfo=timezone.utc),
    )
    assert evidence.classification == SessionClassification.SPECIAL_SESSION
    assert evidence.readiness == SessionReadiness.NOT_COMPLETE


def test_current_weekend_invalid_market_schema_fails_closed():
    target = date(2026, 9, 20)
    evidence = provider(
        [],
        market("not-a-date", "Close"),
        NseFinalCloseUnavailable("unavailable"),
    ).resolve(target, now=datetime(2026, 9, 20, 12, tzinfo=timezone.utc))
    assert evidence.classification == SessionClassification.UNRESOLVED
    assert evidence.readiness == SessionReadiness.UNRESOLVED
    assert evidence.reason_code == "FINAL_ARCHIVE_UNAVAILABLE"


def test_weekday_session_metadata_transport_failure_fails_closed():
    session = MagicMock()
    session.get.side_effect = requests.ConnectionError("transport failure")
    evidence = NseTradingSessionProvider(session=session).resolve(
        date(2026, 9, 18), now=NOW
    )
    assert evidence.classification == SessionClassification.UNRESOLVED
    assert evidence.readiness == SessionReadiness.UNRESOLVED
    assert evidence.reason_code == "SESSION_METADATA_UNAVAILABLE"


def test_unknown_or_ambiguous_status_fails_closed():
    target = date(2026, 9, 18)
    evidence = provider([], market("18-Sep-2026 14:00", "Unknown")).resolve(
        target, now=NOW
    )
    assert evidence.classification == SessionClassification.UNRESOLVED
    assert evidence.readiness == SessionReadiness.UNRESOLVED


def test_previous_session_tuesday_and_saturday_at_0030_ist():
    from zoneinfo import ZoneInfo
    for today, target in ((date(2026, 9, 22), date(2026, 9, 21)),
                          (date(2026, 9, 19), date(2026, 9, 18))):
        now = datetime.combine(today, datetime.min.time(), tzinfo=ZoneInfo("Asia/Kolkata")).replace(minute=30)
        p = provider([], market(target.strftime("%d-%b-%Y") + " 15:30", "Close"))
        evidence = p.previous_session(today, now=now)
        assert evidence.session_date == target
        assert evidence.readiness == SessionReadiness.COMPLETE


def test_previous_session_skips_only_proven_holidays_and_weekends():
    # Monday is an official holiday; Tuesday resolves Friday. Never infer that
    # a weekend is closed merely because archives failed at transport level.
    p = provider([{"tradingDate": "21-Sep-2026"}], market("18-Sep-2026 15:30", "Close"),
                 NseFinalCloseNotPublished("404"))
    evidence = p.previous_session(date(2026, 9, 22), now=datetime(2026, 9, 22, tzinfo=timezone.utc))
    assert evidence.session_date == date(2026, 9, 18)
    assert evidence.readiness == SessionReadiness.COMPLETE
    assert p._final_close_provider.fetch_cash.call_count == 3


def test_previous_session_never_skips_unknown_or_pending_session():
    for error, expected in ((NseFinalCloseUnavailable("transport"), SessionReadiness.UNRESOLVED),
                            (NseFinalCloseNotPublished("404"), SessionReadiness.NOT_COMPLETE)):
        p = provider([], market("18-Sep-2026 15:30", "Close"), error)
        evidence = p.previous_session(date(2026, 9, 22), now=datetime(2026, 9, 22, tzinfo=timezone.utc))
        assert evidence.session_date == date(2026, 9, 21)
        assert evidence.readiness == expected
        assert p._final_close_provider.fetch_cash.call_count == 1


def test_previous_session_preserves_special_sunday_archive():
    p = provider([], market("18-Sep-2026 15:30", "Close"), final_catalog(date(2026, 9, 20)))
    evidence = p.previous_session(date(2026, 9, 21), now=datetime(2026, 9, 21, tzinfo=timezone.utc))
    assert evidence.session_date == date(2026, 9, 20)
    assert evidence.classification == SessionClassification.SPECIAL_SESSION
    assert evidence.readiness == SessionReadiness.COMPLETE
