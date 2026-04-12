"""Top-level engine wiring."""

from __future__ import annotations

import math
from dataclasses import asdict
from typing import Any, Dict, Optional

from .constants import CONSTANTS
from .data import load_and_align_data
from .diagnostics import package_diagnostics
from .eligibility import passes_eligibility_gate
from .enums import InternalState, PublicLabel, ReasonCode, TransitionState
from .exceptions import LogWriteError, MissingIntermediateError, NumericInvalidError, PreviousStateLockError, SchemaValidationError, StatePersistenceError
from .features import compute_features
from .hysteresis import apply_hysteresis
from .logging_utils import log_decision
from .rules import apply_aggregation_rules, assign_provisional_labels, compute_conflicts, compute_veto_flags
from .schema import validate_payload
from .scoring import compute_timeframe_scores
from .types import ErrorInfo, PreviousState


def _confidence(final_label: str, scores, vetoes, provisional) -> float:
    # SPEC TRACE: Section 10 - confidence formula
    if ((provisional.weekly.label == provisional.daily.label == provisional.hourly.label) and provisional.weekly.label != "FLAT"):
        alignment = 1.0
    elif (provisional.weekly.label == provisional.daily.label) and (provisional.hourly.label == "FLAT" or provisional.hourly.label != provisional.weekly.label):
        alignment = 0.5
    else:
        alignment = 0.0

    veto_count = sum([vetoes.shock, vetoes.volatility, vetoes.anomaly, vetoes.liquidity])
    confidence = (
        0.35 * alignment +
        0.25 * abs(scores.aggregate_score) / 50.0 +
        0.20 * max(scores.weekly.quality, 0.0) +
        0.10 * max(scores.daily.quality, 0.0) +
        0.10 * (1 - veto_count / 4)
    )
    return max(0.0, min(1.0, float(confidence)))


def _timeframe_dict(score_block) -> Dict[str, Any]:
    return {
        "label": score_block.label,
        "score": score_block.score,
        "direction": score_block.direction,
        "quality": score_block.quality,
        "noise": score_block.noise,
    }


def public_output(
    instrument: str,
    asof_time: str,
    label: str,
    confidence: float,
    regime_strength: float,
    internal_state: str,
    aggregate_score: Optional[float],
    transition_state: str,
    roll_flag: bool,
    data_quality_warning: bool,
    vetoes,
    timeframes,
    diagnostics,
    errors,
) -> Dict[str, Any]:
    # SPEC TRACE: Section 12 - public_output must conform to the formal JSON schema exactly
    payload = {
        "spec_version": CONSTANTS.spec_version,
        "instrument": instrument,
        "asof_time": asof_time,
        "label": label,
        "confidence": confidence,
        "regime_strength": regime_strength,
        "internal_state": internal_state,
        "aggregate_score": aggregate_score,
        "transition_state": transition_state,
        "roll_flag": roll_flag,
        "data_quality_warning": data_quality_warning,
        "vetoes": {
            "shock": vetoes.shock,
            "volatility": vetoes.volatility,
            "anomaly": vetoes.anomaly,
            "liquidity": vetoes.liquidity,
            "roll": vetoes.roll,
        },
        "timeframes": {
            "weekly": _timeframe_dict(timeframes.weekly),
            "daily": _timeframe_dict(timeframes.daily),
            "hourly": _timeframe_dict(timeframes.hourly),
        },
        "diagnostics": diagnostics,
        "errors": [{"code": e.code, "message": e.message} if isinstance(e, ErrorInfo) else e for e in errors],
    }
    validate_payload(payload)
    return payload


def _emit_unclassifiable(instrument: str, asof_time: str, gate, roll_flag: bool, data_quality_warning: bool) -> Dict[str, Any]:
    from .types import TimeframeScore, ScoreBundle, VetoFlags
    tf = TimeframeScore(label="FLAT", score=None, direction=None, quality=None, noise=None)
    scores = ScoreBundle(weekly=tf, daily=tf, hourly=tf, aggregate_score=None)
    vetoes = VetoFlags(shock=False, volatility=False, anomaly=False, liquidity=False, roll=roll_flag)
    diagnostics = {
        "acceptance_partial": False,
        "missing_bars_adjusted": False,
        "pivot_tie": False,
        "hourly_deterioration": False,
        "conflicts": {"major": False, "score": False, "quality": False},
        "reason_codes": gate.reason_codes,
    }
    return public_output(
        instrument=instrument,
        asof_time=asof_time,
        label="FLAT",
        confidence=0.20 if gate.reason_codes else 0.0,
        regime_strength=0.0,
        internal_state=InternalState.UNCLASSIFIABLE.value,
        aggregate_score=None,
        transition_state=TransitionState.STABLE.value,
        roll_flag=roll_flag,
        data_quality_warning=data_quality_warning,
        vetoes=vetoes,
        timeframes=scores,
        diagnostics=diagnostics,
        errors=gate.errors,
    )


def evaluate_trend(
    instrument: str,
    asof_time: Any,
    calendar: str,
    raw_bars: Dict[str, Any],
    instrument_metadata: Optional[Dict[str, Any]] = None,
    previous_state: Optional[PreviousState] = None,
    log_path: Optional[str] = None,
) -> Dict[str, Any]:
    # SPEC TRACE: Section 15 - reference pseudocode
    instrument_metadata = instrument_metadata or {}
    previous_state = previous_state or PreviousState()
    data = load_and_align_data(instrument, asof_time, calendar, raw_bars, instrument_metadata)
    gate = passes_eligibility_gate(data)
    if gate.passed is False:
        return _emit_unclassifiable(instrument, str(data.asof_time.isoformat()), gate, data.roll_flag, data.data_quality_warning)

    internal_state = InternalState.CLASSIFIABLE.value
    errors = []
    extra_reason_codes: list[str] = []
    try:
        feats = compute_features(data)
        feats.diagnostics["missing_bars_adjusted"] = bool(
            data.weekly.missing_bars_adjusted or data.daily.missing_bars_adjusted or data.hourly.missing_bars_adjusted
        )
        scores = compute_timeframe_scores(feats)
        provisional = assign_provisional_labels(scores)
        vetoes = compute_veto_flags(feats, provisional, scores, roll_flag=data.roll_flag)
        conflicts = compute_conflicts(provisional, scores)
        candidate = apply_aggregation_rules(provisional, scores, vetoes, conflicts)
        final_label, transition_state, previous_state, hysteresis_reasons = apply_hysteresis(
            candidate=candidate,
            previous_state=previous_state,
            scores=scores,
            vetoes=vetoes,
            asof_time=data.asof_time,
        )
        extra_reason_codes.extend(hysteresis_reasons)
        confidence = _confidence(final_label, scores, vetoes, provisional)
        regime_strength = math.tanh(abs(scores.aggregate_score) / 40.0) if internal_state == "CLASSIFIABLE" else 0.0
        diagnostics = package_diagnostics(feats, vetoes, conflicts, transition_state, extra_reason_codes=extra_reason_codes)
        payload = public_output(
            instrument=instrument,
            asof_time=str(data.asof_time.isoformat()),
            label=final_label,
            confidence=confidence,
            regime_strength=regime_strength,
            internal_state=internal_state,
            aggregate_score=scores.aggregate_score,
            transition_state=transition_state,
            roll_flag=data.roll_flag,
            data_quality_warning=data.data_quality_warning,
            vetoes=vetoes,
            timeframes=scores,
            diagnostics=diagnostics,
            errors=errors,
        )
        try:
            log_decision(instrument, str(data.asof_time.isoformat()), payload, log_path=log_path)
        except LogWriteError as exc:
            payload["diagnostics"]["reason_codes"].append(ReasonCode.RUNTIME_LOG_WRITE_FAILED.value)
        return payload
    except (MissingIntermediateError, NumericInvalidError) as exc:
        gate.reason_codes.append(ReasonCode.MISSING_INTERMEDIATE_LOAD_BEARING.value if isinstance(exc, MissingIntermediateError) else ReasonCode.NUMERIC_INVALID.value)
        gate.errors.append(ErrorInfo(code=gate.reason_codes[-1], message=str(exc)))
        return _emit_unclassifiable(instrument, str(data.asof_time.isoformat()), gate, data.roll_flag, data.data_quality_warning)
    except SchemaValidationError as exc:
        raise
