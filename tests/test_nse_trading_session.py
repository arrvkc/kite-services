from datetime import date, datetime, timezone
from unittest.mock import MagicMock

from services.nse_final_close import (
    NseFinalCloseCatalog,
    NseFinalCloseNotPublished,
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


def test_weekday_holiday_and_normal_weekend_are_clean_no_session():
    holiday = date(2026, 9, 14)
    holiday_row = {"tradingDate": "14-Sep-2026"}
    prior = market("11-Sep-2026 15:30", "Close")
    holiday_evidence = provider(
        [holiday_row], prior, NseFinalCloseNotPublished("missing")
    ).resolve(holiday, now=NOW)
    weekend = provider(
        [], prior, NseFinalCloseNotPublished("missing")
    ).resolve(date(2026, 9, 19), now=NOW)
    assert holiday_evidence.classification == SessionClassification.NO_TRADING_SESSION
    assert holiday_evidence.readiness == SessionReadiness.NO_SESSION
    assert weekend.classification == SessionClassification.NO_TRADING_SESSION
    assert weekend.readiness == SessionReadiness.NO_SESSION


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


def test_unknown_or_ambiguous_status_fails_closed():
    target = date(2026, 9, 18)
    evidence = provider([], market("18-Sep-2026 14:00", "Unknown")).resolve(
        target, now=NOW
    )
    assert evidence.classification == SessionClassification.UNRESOLVED
    assert evidence.readiness == SessionReadiness.UNRESOLVED
