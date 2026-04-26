from __future__ import annotations

import math
from typing import Any

from .constants import (
    LABEL_DOWN,
    LABEL_FLAT,
    LABEL_UP,
    REASON_GATE_INSUFFICIENT_HISTORY,
    REASON_GATE_INVALID_DTE,
    REASON_GATE_INVALID_NUMERIC,
    REASON_GATE_MISSING_REQUIRED_FIELD,
    REASON_GATE_NOT_IN_UNIVERSE,
    REASON_GATE_UPSTREAM_UNCLASSIFIABLE,
    STATE_CLASSIFIABLE,
)
from .models import InvalidEvaluationRecord, ValidatedBundle

def _error(code: str, message: str) -> dict[str, str]:
    return {"code": code, "message": message}

def _is_finite_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and math.isfinite(value)

def _is_valid_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)

def validate_inputs(payload: dict[str, Any]) -> tuple[ValidatedBundle | None, InvalidEvaluationRecord | None]:
    reason_codes: list[str] = []
    errors: list[dict[str, str]] = []
    required = [
        "instrument",
        "asof_time",
        "label",
        "confidence",
        "aggregate_score",
        "internal_state",
        "trend_history_w5",
        "dte_near_month",
        "next_month_available",
        "in_universe",
    ]
    for field_name in required:
        if field_name not in payload:
            if REASON_GATE_MISSING_REQUIRED_FIELD not in reason_codes:
                reason_codes.append(REASON_GATE_MISSING_REQUIRED_FIELD)
            errors.append(_error(REASON_GATE_MISSING_REQUIRED_FIELD, f"Missing required field: {field_name}"))
    if errors:
        return None, InvalidEvaluationRecord(
            instrument=str(payload.get("instrument", "")),
            asof_time=payload.get("asof_time"),
            reason_codes=reason_codes,
            errors=errors,
            input_snapshot={k: payload.get(k) for k in ["label", "confidence", "aggregate_score"]},
        )
    if payload["internal_state"] != STATE_CLASSIFIABLE:
        reason_codes.append(REASON_GATE_UPSTREAM_UNCLASSIFIABLE)
        errors.append(_error(REASON_GATE_UPSTREAM_UNCLASSIFIABLE, "internal_state must be CLASSIFIABLE"))
    label = payload["label"]
    if label not in {LABEL_UP, LABEL_FLAT, LABEL_DOWN}:
        if REASON_GATE_MISSING_REQUIRED_FIELD not in reason_codes:
            reason_codes.append(REASON_GATE_MISSING_REQUIRED_FIELD)
        errors.append(_error(REASON_GATE_MISSING_REQUIRED_FIELD, "label must be UP, FLAT, or DOWN"))
    confidence = payload["confidence"]
    aggregate_score = payload["aggregate_score"]
    if not _is_finite_number(confidence) or not (0.0 <= float(confidence) <= 1.0):
        if REASON_GATE_INVALID_NUMERIC not in reason_codes:
            reason_codes.append(REASON_GATE_INVALID_NUMERIC)
        errors.append(_error(REASON_GATE_INVALID_NUMERIC, "confidence must be finite and within [0,1]"))
    if not _is_finite_number(aggregate_score):
        if REASON_GATE_INVALID_NUMERIC not in reason_codes:
            reason_codes.append(REASON_GATE_INVALID_NUMERIC)
        errors.append(_error(REASON_GATE_INVALID_NUMERIC, "aggregate_score must be finite"))
    history = payload["trend_history_w5"]
    if not isinstance(history, list) or len(history) < 5:
        if REASON_GATE_INSUFFICIENT_HISTORY not in reason_codes:
            reason_codes.append(REASON_GATE_INSUFFICIENT_HISTORY)
        errors.append(_error(REASON_GATE_INSUFFICIENT_HISTORY, "At least 5 completed daily evaluations are required"))
    else:
        for index, row in enumerate(history[:5]):
            row_required = ["label", "confidence", "aggregate_score"]
            for field_name in row_required:
                if field_name not in row:
                    if REASON_GATE_MISSING_REQUIRED_FIELD not in reason_codes:
                        reason_codes.append(REASON_GATE_MISSING_REQUIRED_FIELD)
                    errors.append(_error(REASON_GATE_MISSING_REQUIRED_FIELD, f"History row {index} missing field: {field_name}"))
            if "confidence" in row and (not _is_finite_number(row["confidence"]) or not (0.0 <= float(row["confidence"]) <= 1.0)):
                if REASON_GATE_INVALID_NUMERIC not in reason_codes:
                    reason_codes.append(REASON_GATE_INVALID_NUMERIC)
                errors.append(_error(REASON_GATE_INVALID_NUMERIC, f"History row {index} has invalid confidence"))
            if "aggregate_score" in row and not _is_finite_number(row["aggregate_score"]):
                if REASON_GATE_INVALID_NUMERIC not in reason_codes:
                    reason_codes.append(REASON_GATE_INVALID_NUMERIC)
                errors.append(_error(REASON_GATE_INVALID_NUMERIC, f"History row {index} has invalid aggregate_score"))
            if "label" in row and row["label"] not in {LABEL_UP, LABEL_FLAT, LABEL_DOWN}:
                if REASON_GATE_MISSING_REQUIRED_FIELD not in reason_codes:
                    reason_codes.append(REASON_GATE_MISSING_REQUIRED_FIELD)
                errors.append(_error(REASON_GATE_MISSING_REQUIRED_FIELD, f"History row {index} has invalid label"))
    dte_near = payload["dte_near_month"]
    if not _is_valid_int(dte_near) or dte_near < 0:
        if REASON_GATE_INVALID_DTE not in reason_codes:
            reason_codes.append(REASON_GATE_INVALID_DTE)
        errors.append(_error(REASON_GATE_INVALID_DTE, "dte_near_month must be an integer >= 0"))
    next_month_available = payload["next_month_available"]
    if not isinstance(next_month_available, bool):
        if REASON_GATE_INVALID_DTE not in reason_codes:
            reason_codes.append(REASON_GATE_INVALID_DTE)
        errors.append(_error(REASON_GATE_INVALID_DTE, "next_month_available must be boolean"))
    dte_next = payload.get("dte_next_month")
    if isinstance(next_month_available, bool) and next_month_available:
        if not _is_valid_int(dte_next) or dte_next < 0:
            if REASON_GATE_INVALID_DTE not in reason_codes:
                reason_codes.append(REASON_GATE_INVALID_DTE)
            errors.append(_error(REASON_GATE_INVALID_DTE, "dte_next_month must be an integer >= 0 when next_month_available is true"))
    if not bool(payload["in_universe"]):
        reason_codes.append(REASON_GATE_NOT_IN_UNIVERSE)
        errors.append(_error(REASON_GATE_NOT_IN_UNIVERSE, "Instrument must be in the live NSE stock derivatives universe"))
    if errors:
        return None, InvalidEvaluationRecord(
            instrument=str(payload.get("instrument", "")),
            asof_time=payload.get("asof_time"),
            reason_codes=reason_codes,
            errors=errors,
            input_snapshot={k: payload.get(k) for k in ["label", "confidence", "aggregate_score", "dte_near_month", "next_month_available", "dte_next_month"]},
        )
    bundle = ValidatedBundle(
        instrument=str(payload["instrument"]),
        asof_time=str(payload["asof_time"]),
        label=str(label),
        confidence=float(confidence),
        aggregate_score=float(aggregate_score),
        internal_state=str(payload["internal_state"]),
        trend_history_w5=history[-5:],
        dte_near_month=dte_near,
        next_month_available=next_month_available,
        dte_next_month=dte_next if next_month_available else None,
        in_universe=bool(payload["in_universe"]),
        prior_committed_state=payload.get("prior_committed_state"),
    )
    return bundle, None
