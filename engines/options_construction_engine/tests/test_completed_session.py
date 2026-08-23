from __future__ import annotations

from copy import deepcopy
from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from engines.options_construction_engine.completed_session import (
    CONTRACT_IDENTITY,
    CompletedSessionEvidenceError,
    construct_from_completed_session_market_facts,
    normalize_completed_session_instrument,
    resolve_latest_completed_session,
)
from engines.options_construction_engine.constants import (
    COMPLETED_SESSION_HISTORICAL_PRICE_MODE,
    COMPLETED_SESSION_LIQUIDITY_CHECK_PASSED,
    HISTORICAL_SESSION_OI_TOO_LOW,
    HISTORICAL_SESSION_OI_UNAVAILABLE,
    HISTORICAL_SESSION_PRICE_UNAVAILABLE,
    HISTORICAL_SESSION_VOLUME_TOO_LOW,
    LIQUIDITY_MODE_COMPLETED_SESSION_HISTORICAL,
    STATUS_CONSTRUCTED,
    STATUS_REJECTED,
)


IST = ZoneInfo("Asia/Kolkata")
RESOLVED_AT = datetime(2026, 8, 23, 12, 0, tzinfo=IST)


def candle(close, *, at="2026-08-21T15:25:00+05:30", volume=200, oi=1000):
    return {"date": at, "close": close, "volume": volume, "oi": oi}


def instrument(token, symbol, strike):
    return {
        "instrument_token": token,
        "tradingsymbol": symbol,
        "exchange": "NFO",
        "name": "ABC",
        "expiry": "2026-08-27",
        "strike": strike,
        "instrument_type": "CE",
        "lot_size": 100,
    }


def request(*, short_candles=None, long_candles=None, underlying_candles=None):
    return construct_from_completed_session_market_facts(
        {
            "instrument": "ABC",
            "strategy_family": "BEAR_CALL_SPREAD",
            "contract_month_selection": "NEAR_MONTH",
            "final_strategy_strength": 70,
        },
        underlying_instrument={
            "instrument_token": 99,
            "tradingsymbol": "ABC",
            "exchange": "NSE",
            "name": "ABC",
        },
        underlying_candles=underlying_candles or [candle(100, volume=1000, oi=None)],
        option_instruments=[instrument(1, "ABC105CE", 105), instrument(2, "ABC110CE", 110)],
        option_candles={
            "1": short_candles if short_candles is not None else [candle(3)],
            "2": long_candles if long_candles is not None else [candle(1)],
        },
        resolved_at=RESOLVED_AT,
        retrieved_at=RESOLVED_AT,
        source_identity="broker-candle-source-v1",
        engine_source_identity="fee0bfec-test-source",
    )


def test_weekend_resolves_latest_completed_session_from_broker_candles():
    session = resolve_latest_completed_session(
        [candle(100), candle(99, at="2026-08-20T15:25:00+05:30")],
        resolved_at=RESOLVED_AT,
    )
    assert session["session_date"] == "2026-08-21"
    assert session["session_close"] == "2026-08-21T15:30:00+05:30"
    assert session["resolution_source"] == "BROKER_UNDERLYING_HISTORICAL_CANDLES"


def test_daily_broker_candle_is_bound_to_its_completed_session_close():
    session = resolve_latest_completed_session(
        [candle(100, at="2026-08-21T00:00:00+05:30")],
        resolved_at=RESOLVED_AT,
    )
    daily = dict(candle(3, at="2026-08-21T00:00:00+05:30"), interval="day")
    evidence = normalize_completed_session_instrument(
        instrument(1, "ABC105CE", 105), [daily], session,
        source_identity="source", retrieved_at=RESOLVED_AT,
    )
    assert evidence["session_last_trade_at"].isoformat() == "2026-08-21T15:30:00+05:30"


def test_session_resolution_fails_closed_without_completed_candle():
    with pytest.raises(CompletedSessionEvidenceError, match="LATEST_COMPLETED_SESSION_UNAVAILABLE"):
        resolve_latest_completed_session(
            [candle(100, at="2026-08-24T10:00:00+05:30")],
            resolved_at=RESOLVED_AT,
        )


def test_normalization_uses_session_last_positive_trade_and_session_totals():
    session = resolve_latest_completed_session([candle(100)], resolved_at=RESOLVED_AT)
    evidence = normalize_completed_session_instrument(
        instrument(1, "ABC105CE", 105),
        [
            candle(2.5, at="2026-08-21T09:20:00+05:30", volume=60, oi=700),
            candle(3.0, at="2026-08-21T15:25:00+05:30", volume=90, oi=900),
        ],
        session,
        source_identity="source",
        retrieved_at=RESOLVED_AT,
    )
    assert str(evidence["session_last_trade_price"]) == "3.0"
    assert evidence["session_volume"] == 150
    assert evidence["session_oi"] == 900
    assert evidence["session_last_trade_at"].isoformat() == "2026-08-21T15:25:00+05:30"


def test_completed_session_constructs_with_no_synthetic_bid_ask():
    result = request()
    assert result["status"] == STATUS_CONSTRUCTED
    assert result["evidence_mode"] == LIQUIDITY_MODE_COMPLETED_SESSION_HISTORICAL
    assert result["execution_ready"] is False
    assert COMPLETED_SESSION_LIQUIDITY_CHECK_PASSED in result["reason_codes"]
    assert COMPLETED_SESSION_HISTORICAL_PRICE_MODE in result["reason_codes"]
    assert all(leg["bid_price"] is None and leg["ask_price"] is None for leg in result["legs"])
    assert [leg["historical_reference_price"] for leg in result["legs"]] == [3.0, 1.0]


def test_completed_session_price_drives_existing_engine_economics():
    result = request()
    assert result["construction_result"]["net_premium"] == 2.0
    assert result["construction_result"]["max_loss_per_lot"] == 300.0
    assert result["construction_result"]["max_profit_per_lot"] == 200.0


@pytest.mark.parametrize(
    ("short_candles", "reason"),
    [
        ([candle(0, volume=200, oi=1000)], HISTORICAL_SESSION_PRICE_UNAVAILABLE),
        ([candle(3, volume=99, oi=1000)], HISTORICAL_SESSION_VOLUME_TOO_LOW),
        ([candle(3, volume=200, oi=499)], HISTORICAL_SESSION_OI_TOO_LOW),
        ([candle(3, volume=200, oi=None)], HISTORICAL_SESSION_OI_UNAVAILABLE),
    ],
)
def test_completed_session_liquidity_failures_are_explicit(short_candles, reason):
    result = request(short_candles=short_candles)
    assert result["status"] == STATUS_REJECTED
    assert reason in result["reason_codes"]


def test_missing_latest_session_option_evidence_does_not_search_backwards():
    previous = [candle(3, at="2026-08-20T15:25:00+05:30")]
    result = request(short_candles=previous)
    assert result["status"] == STATUS_REJECTED
    assert HISTORICAL_SESSION_PRICE_UNAVAILABLE in result["reason_codes"]
    assert result["market_session_identity"].startswith("NFO:2026-08-21:")


def test_current_open_interest_is_not_an_accepted_fallback():
    current_only = [dict(candle(3, oi=None), current_open_interest=50000)]
    result = request(short_candles=current_only)
    assert result["status"] == STATUS_REJECTED
    assert HISTORICAL_SESSION_OI_UNAVAILABLE in result["reason_codes"]


def test_market_evidence_is_complete_reconstructible_and_source_bound():
    result = request()
    evidence = result["market_evidence"]
    assert evidence["contract_identity"] == CONTRACT_IDENTITY
    assert evidence["engine_source_identity"] == "fee0bfec-test-source"
    assert evidence["source_identity"] == "broker-candle-source-v1"
    assert evidence["session"]["session_identity"] == result["market_session_identity"]
    assert evidence["engine_config"]["historical_bid_ask_spread"] == "NOT_EVALUATED"
    assert evidence["engine_config"]["live_freshness"] == "NOT_APPLICABLE_COMPLETED_SESSION"
    assert len(result["market_evidence_identity"]) == 64


def test_identical_facts_are_deterministic_and_input_is_not_mutated():
    short = [candle(3)]
    original = deepcopy(short)
    first = request(short_candles=short)
    second = request(short_candles=short)
    assert first == second
    assert short == original


def test_changed_raw_candle_changes_market_evidence_identity():
    first = request()
    second = request(short_candles=[dict(candle(3), open=2.9)])
    assert first["market_evidence_identity"] != second["market_evidence_identity"]


def test_malformed_timezone_naive_candle_fails_closed():
    with pytest.raises(CompletedSessionEvidenceError, match="timezone-aware"):
        request(underlying_candles=[candle(100, at="2026-08-21T15:25:00")])


def test_legacy_and_live_engine_tests_remain_separate_from_completed_mode():
    result = request()
    assert result["evidence_mode"] == "COMPLETED_SESSION_HISTORICAL"
    assert result["construction_result"]["execution_ready"] is False
