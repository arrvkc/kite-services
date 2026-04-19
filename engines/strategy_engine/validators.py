"""Input validation and hard gating."""
from __future__ import annotations

import math
from typing import List

from . import constants as c
from .models import ErrorObject, GateResult, StrategyInput, ValidatedInputBundle


class ValidationError(Exception):
    """Raised when validation cannot produce a deterministic bundle."""


def _is_finite_number(value: object) -> bool:
    return isinstance(value, (int, float)) and math.isfinite(value)


def validate_inputs(raw: StrategyInput) -> ValidatedInputBundle:
    """Validate structural presence and shape without applying business gates."""
    if not raw.instrument:
        raise ValidationError("instrument must be non-empty")
    if raw.latest_payload.instrument != raw.instrument:
        raise ValidationError("latest payload instrument mismatch")
    if raw.latest_payload.label not in c.UPSTREAM_LABELS:
        raise ValidationError("invalid upstream label")
    if raw.latest_payload.internal_state not in c.UPSTREAM_STATES:
        raise ValidationError("invalid upstream internal_state")
    if not isinstance(raw.next_month_available, bool):
        raise ValidationError("next_month_available must be boolean")
    if not isinstance(raw.dte_near_month, int):
        raise ValidationError("dte_near_month must be integer")
    if raw.next_month_available and not isinstance(raw.dte_next_month, int):
        raise ValidationError("dte_next_month must be integer when next month is available")
    if (not raw.next_month_available) and raw.dte_next_month is not None:
        # Keep strict to mirror the schema contract.
        raise ValidationError("dte_next_month must be null when next month is unavailable")
    return ValidatedInputBundle(
        instrument=raw.instrument,
        latest_payload=raw.latest_payload,
        trend_history_w5=raw.trend_history_w5,
        dte_near_month=raw.dte_near_month,
        next_month_available=raw.next_month_available,
        dte_next_month=raw.dte_next_month,
        in_universe=raw.in_universe,
        duplicate_payload=raw.duplicate_payload,
        is_completed_daily_run=raw.is_completed_daily_run,
    )


def passes_strategy_gate(bundle: ValidatedInputBundle) -> GateResult:
    """Apply exact hard gates from the specification."""
    reason_codes: List[str] = []
    errors: List[ErrorObject] = []

    if bundle.duplicate_payload:
        return GateResult(
            passed=False,
            reason_codes=["GATE_MISSING_REQUIRED_FIELD"],
            errors=[ErrorObject(code="DUPLICATE_PAYLOAD", message="Duplicate final payload for instrument and asof_time")],
        )

    payload = bundle.latest_payload
    if payload.internal_state != "CLASSIFIABLE":
        reason_codes.append("GATE_UPSTREAM_UNCLASSIFIABLE")

    if not bundle.is_completed_daily_run:
        reason_codes.append("GATE_MISSING_REQUIRED_FIELD")

    if not bundle.in_universe:
        reason_codes.append("GATE_NOT_IN_UNIVERSE")

    if len(bundle.trend_history_w5) < 5:
        reason_codes.append("GATE_INSUFFICIENT_HISTORY")

    required_payload_fields_present = (
        payload.label is not None
        and payload.confidence is not None
        and payload.aggregate_score is not None
        and payload.asof_time
    )
    if not required_payload_fields_present:
        reason_codes.append("GATE_MISSING_REQUIRED_FIELD")

    if not _is_finite_number(payload.confidence) or not (0.0 <= float(payload.confidence) <= 1.0):
        reason_codes.append("GATE_INVALID_NUMERIC")
    if payload.aggregate_score is None or not _is_finite_number(payload.aggregate_score):
        reason_codes.append("GATE_INVALID_NUMERIC")

    if not isinstance(bundle.dte_near_month, int) or bundle.dte_near_month < 0:
        reason_codes.append("GATE_INVALID_DTE")
    if not isinstance(bundle.next_month_available, bool):
        reason_codes.append("GATE_MISSING_REQUIRED_FIELD")
    if bundle.next_month_available and (
        not isinstance(bundle.dte_next_month, int) or bundle.dte_next_month < 0
    ):
        reason_codes.append("GATE_INVALID_DTE")

    for row in bundle.trend_history_w5[:5]:
        if row.label not in c.UPSTREAM_LABELS:
            reason_codes.append("GATE_MISSING_REQUIRED_FIELD")
            break
        if not _is_finite_number(row.confidence) or not (0.0 <= row.confidence <= 1.0):
            reason_codes.append("GATE_INVALID_NUMERIC")
            break
        if not _is_finite_number(row.aggregate_score):
            reason_codes.append("GATE_INVALID_NUMERIC")
            break

    if reason_codes:
        uniq = []
        for code in reason_codes:
            if code not in uniq:
                uniq.append(code)
        for code in uniq:
            errors.append(ErrorObject(code=code, message=code))
        return GateResult(False, uniq, errors)
    return GateResult(True, [])
