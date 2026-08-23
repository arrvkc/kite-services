"""Pure completed-session evidence contract for non-executable construction analysis.

The caller owns broker authentication and read-only candle acquisition. This
module resolves one completed exchange session, normalizes every supplied
instrument to that session, and invokes the same ``OptionsConstructionEngine``
used by live construction. It has no broker SDK, credentials, CLI, or order
surface.
"""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, time
from decimal import Decimal
import hashlib
import json
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo

from .constants import (
    COMPLETED_SESSION_EVIDENCE_VERSION,
    IMPLEMENTATION_VERSION,
    LIQUIDITY_MODE_COMPLETED_SESSION_HISTORICAL,
)
from .contract import (
    construct_from_supplied_market_facts,
    prepare_supplied_option_market_context,
)
from .models import OptionsConstructionConfig


CONTRACT_IDENTITY = "EAJEE_OPTIONS_COMPLETED_SESSION_CONSTRUCTION"
SESSION_TIMEZONE = "Asia/Kolkata"
SESSION_OPEN = time(9, 15)
SESSION_CLOSE = time(15, 30)
SESSION_RESOLUTION_SOURCE = "BROKER_UNDERLYING_HISTORICAL_CANDLES"


class CompletedSessionEvidenceError(ValueError):
    """A coherent completed-session observation cannot be established."""


def _instant(value: Any) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value or "").strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError as exc:
            raise CompletedSessionEvidenceError(
                "Historical candle timestamp is invalid."
            ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise CompletedSessionEvidenceError(
            "Historical candle timestamp must be timezone-aware."
        )
    return parsed


def _whole(value: Any, *, allow_none: bool = False) -> int | None:
    if value is None and allow_none:
        return None
    try:
        number = Decimal(str(value))
    except Exception as exc:
        raise CompletedSessionEvidenceError("Historical whole-number fact is invalid.") from exc
    if not number.is_finite() or number < 0 or number != number.to_integral_value():
        raise CompletedSessionEvidenceError("Historical whole-number fact is invalid.")
    return int(number)


def _positive(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        number = Decimal(str(value))
    except Exception as exc:
        raise CompletedSessionEvidenceError("Historical price is invalid.") from exc
    if not number.is_finite() or number <= 0:
        return None
    return number


def _json_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, float):
        return format(Decimal(str(value)), "f")
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    return value


def _fingerprint(value: Any) -> str:
    payload = json.dumps(
        _json_value(value), sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def resolve_latest_completed_session(
    reference_candles: Sequence[Mapping[str, Any]],
    *,
    resolved_at: datetime,
    exchange: str = "NFO",
) -> dict[str, Any]:
    """Resolve the latest completed session evidenced by reference candles."""

    resolved = _instant(resolved_at).astimezone(ZoneInfo(SESSION_TIMEZONE))
    observed_dates = sorted({
        _instant(item.get("date")).astimezone(ZoneInfo(SESSION_TIMEZONE)).date()
        for item in reference_candles
        if isinstance(item, Mapping) and item.get("date") is not None
    })
    completed = []
    for session_date in observed_dates:
        close_at = datetime.combine(
            session_date, SESSION_CLOSE, tzinfo=ZoneInfo(SESSION_TIMEZONE)
        )
        if close_at <= resolved:
            completed.append(session_date)
    if not completed:
        raise CompletedSessionEvidenceError("LATEST_COMPLETED_SESSION_UNAVAILABLE")
    session_date = completed[-1]
    open_at = datetime.combine(
        session_date, SESSION_OPEN, tzinfo=ZoneInfo(SESSION_TIMEZONE)
    )
    close_at = datetime.combine(
        session_date, SESSION_CLOSE, tzinfo=ZoneInfo(SESSION_TIMEZONE)
    )
    return {
        "exchange": str(exchange).upper(),
        "timezone": SESSION_TIMEZONE,
        "session_date": session_date.isoformat(),
        "session_open": open_at.isoformat(),
        "session_close": close_at.isoformat(),
        "resolution_source": SESSION_RESOLUTION_SOURCE,
        "resolved_at": resolved.isoformat(),
        "session_identity": "{0}:{1}:{2}:{3}".format(
            str(exchange).upper(), session_date.isoformat(),
            SESSION_OPEN.isoformat(timespec="minutes"),
            SESSION_CLOSE.isoformat(timespec="minutes"),
        ),
    }


def _session_candles(
    candles: Sequence[Mapping[str, Any]], session: Mapping[str, Any]
) -> list[tuple[datetime, Mapping[str, Any]]]:
    open_at = _instant(session["session_open"])
    close_at = _instant(session["session_close"])
    rows = []
    for raw in candles:
        if not isinstance(raw, Mapping) or raw.get("date") is None:
            continue
        observed_at = _instant(raw["date"]).astimezone(open_at.tzinfo)
        if str(raw.get("interval") or "").lower() == "day":
            if observed_at.date() != open_at.date():
                continue
            # A broker daily candle is the aggregate for this exchange session;
            # its conventional midnight timestamp identifies the session date,
            # while the completed evidence instant is the governed session close.
            observed_at = close_at
        if open_at <= observed_at <= close_at:
            rows.append((observed_at, raw))
    return sorted(rows, key=lambda item: item[0])


def normalize_completed_session_instrument(
    instrument: Mapping[str, Any],
    candles: Sequence[Mapping[str, Any]],
    session: Mapping[str, Any],
    *,
    source_identity: str,
    retrieved_at: datetime,
    require_liquidity: bool = True,
) -> dict[str, Any]:
    """Normalize one instrument without borrowing facts from another session."""

    rows = _session_candles(candles, session)
    positive_trades = [
        (observed_at, raw, _positive(raw.get("close")))
        for observed_at, raw in rows
        if (_whole(raw.get("volume") or 0) or 0) > 0
        and _positive(raw.get("close")) is not None
    ]
    final_trade = positive_trades[-1] if positive_trades else None
    oi_rows = [
        (observed_at, _whole(raw.get("oi"), allow_none=True))
        for observed_at, raw in rows
        if raw.get("oi") is not None
    ]
    final_oi = oi_rows[-1] if oi_rows else None
    volume = sum((_whole(raw.get("volume") or 0) or 0) for _, raw in rows)
    evidence = {
        "instrument_token": str(instrument.get("instrument_token") or ""),
        "tradingsymbol": str(instrument.get("tradingsymbol") or ""),
        "exchange": str(instrument.get("exchange") or session["exchange"]),
        "underlying": instrument.get("underlying") or instrument.get("name"),
        "expiry": None if instrument.get("expiry") is None else str(instrument["expiry"]),
        "strike": instrument.get("strike"),
        "option_type": instrument.get("option_type") or instrument.get("instrument_type"),
        "lot_size": instrument.get("lot_size"),
        "session_identity": session["session_identity"],
        "session_date": session["session_date"],
        "session_open": session["session_open"],
        "session_close": session["session_close"],
        "session_last_trade_price": None if final_trade is None else final_trade[2],
        "session_last_trade_at": None if final_trade is None else final_trade[0],
        "session_volume": volume,
        "session_oi": None if final_oi is None else final_oi[1],
        "session_oi_at": None if final_oi is None else final_oi[0],
        "source": source_identity,
        "retrieved_at": _instant(retrieved_at),
        "raw_evidence_fingerprint": _fingerprint(rows),
    }
    if not evidence["instrument_token"] or not evidence["tradingsymbol"]:
        raise CompletedSessionEvidenceError("Historical instrument identity is incomplete.")
    if not require_liquidity and evidence["session_last_trade_price"] is None:
        raise CompletedSessionEvidenceError("UNDERLYING_SESSION_PRICE_UNAVAILABLE")
    return evidence


def construct_from_completed_session_market_facts(
    strategy_context: Mapping[str, Any],
    *,
    underlying_instrument: Mapping[str, Any],
    underlying_candles: Sequence[Mapping[str, Any]],
    option_instruments: Sequence[Mapping[str, Any]],
    option_candles: Mapping[str, Sequence[Mapping[str, Any]]],
    resolved_at: datetime,
    retrieved_at: datetime,
    source_identity: str,
    engine_source_identity: str,
) -> dict[str, Any]:
    """Construct against one reconstructible latest-completed-session observation."""

    if not source_identity or not engine_source_identity:
        raise CompletedSessionEvidenceError("Immutable evidence source identity is required.")
    session = resolve_latest_completed_session(
        underlying_candles, resolved_at=resolved_at,
        exchange="NFO",
    )
    underlying = normalize_completed_session_instrument(
        underlying_instrument,
        underlying_candles,
        session,
        source_identity=source_identity,
        retrieved_at=retrieved_at,
        require_liquidity=False,
    )
    normalized_options = []
    for instrument in option_instruments:
        token = str(instrument.get("instrument_token") or "")
        normalized_options.append(
            normalize_completed_session_instrument(
                instrument,
                option_candles.get(token, ()),
                session,
                source_identity=source_identity,
                retrieved_at=retrieved_at,
            )
        )
    option_chain = []
    for evidence in normalized_options:
        option_chain.append({
            "tradingsymbol": evidence["tradingsymbol"],
            "instrument_token": evidence["instrument_token"],
            "expiry": evidence["expiry"],
            "strike": evidence["strike"],
            "option_type": evidence["option_type"],
            "bid_price": None,
            "ask_price": None,
            "last_price": evidence["session_last_trade_price"],
            "historical_reference_price": evidence["session_last_trade_price"],
            "open_interest": evidence["session_oi"],
            "volume": evidence["session_volume"],
            "delta": None,
            "data_timestamp": session["session_close"],
            "underlying": strategy_context["instrument"],
            "lot_size": evidence["lot_size"],
            "historical_last_trade_at": evidence["session_last_trade_at"],
            "historical_oi_at": evidence["session_oi_at"],
            "historical_session_identity": session["session_identity"],
            "pricing_source": "COMPLETED_SESSION_LAST_TRADED_PRICE",
        })
    prepared = prepare_supplied_option_market_context(option_chain)
    payload = deepcopy(dict(strategy_context))
    payload.update({
        "asof_time": session["session_close"],
        "underlying_spot_price": underlying["session_last_trade_price"],
        "strike_step": prepared["strike_step"],
        "lot_size": prepared["lot_size"],
    })
    normalized_chain = list(prepared["option_chain"])
    config = OptionsConstructionConfig(
        liquidity_mode=LIQUIDITY_MODE_COMPLETED_SESSION_HISTORICAL,
        freshness_policy=COMPLETED_SESSION_EVIDENCE_VERSION,
        implementation_build_hash=engine_source_identity,
    )
    result = construct_from_supplied_market_facts(
        payload,
        normalized_chain,
        config=config,
    )
    evidence = {
        "contract_identity": CONTRACT_IDENTITY,
        "engine_contract_version": IMPLEMENTATION_VERSION,
        "evidence_contract_version": COMPLETED_SESSION_EVIDENCE_VERSION,
        "engine_source_identity": engine_source_identity,
        "source_identity": source_identity,
        "session": session,
        "underlying": underlying,
        "option_chain": normalized_options,
        "engine_config": {
            "liquidity_mode": config.liquidity_mode,
            "min_open_interest": config.min_open_interest,
            "min_volume": config.min_volume,
            "historical_bid_ask_spread": "NOT_EVALUATED",
            "live_freshness": "NOT_APPLICABLE_COMPLETED_SESSION",
        },
        "construction_input": {
            "strategy_context": payload,
            "option_chain": normalized_chain,
        },
    }
    evidence_json = _json_value(evidence)
    evidence_fingerprint = _fingerprint(evidence_json)
    output_fingerprint = _fingerprint(result)
    return {
        "status": result.get("construction_status"),
        "family": result.get("strategy_family"),
        "expiry": result.get("expiry"),
        "legs": result.get("legs", []),
        "quantity_lot_size": prepared["lot_size"],
        "construction_score": result.get("construction_score"),
        "construction_fingerprint": output_fingerprint,
        "evidence_mode": LIQUIDITY_MODE_COMPLETED_SESSION_HISTORICAL,
        "evidence_contract_version": COMPLETED_SESSION_EVIDENCE_VERSION,
        "market_session_identity": session["session_identity"],
        "market_evidence_identity": evidence_fingerprint,
        "policy_version": IMPLEMENTATION_VERSION,
        "engine_source_identity": engine_source_identity,
        "reason_codes": result.get("reason_codes", []),
        "execution_ready": False,
        "construction_result": result,
        "market_evidence": evidence_json,
    }
