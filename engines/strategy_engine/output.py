"""Payload construction and minimal schema validation."""
from __future__ import annotations

from typing import Dict, List

from . import constants as c
from .models import ErrorObject, HistoryMetrics, StrategyOutputPayload, ValidatedInputBundle


OUTPUT_SCHEMA: Dict[str, object] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$id": "https://example.internal/schemas/strategy-engine-v1.2.json",
    "title": "Strategy Engine v1.2 Output",
    "type": "object",
}


def public_output(
    bundle: ValidatedInputBundle,
    metrics: HistoryMetrics | None,
    strategy_family: str,
    contract_month_selection: str,
    final_strategy_strength: int,
    transition_state: str,
    reason_codes: List[str],
    errors: List[ErrorObject],
) -> StrategyOutputPayload:
    """Create the public payload."""
    snapshot = {
        "label": bundle.latest_payload.label,
        "confidence": bundle.latest_payload.confidence,
        "aggregate_score": bundle.latest_payload.aggregate_score,
        "dte_near_month": bundle.dte_near_month,
        "next_month_available": bundle.next_month_available,
        "dte_next_month": bundle.dte_next_month,
        "bull_count_5": metrics.bull_count_5 if metrics else 0,
        "bear_count_5": metrics.bear_count_5 if metrics else 0,
        "flat_count_5": metrics.flat_count_5 if metrics else 0,
        "mean_score_3": metrics.mean_score_3 if metrics else 0.0,
        "mean_conf_3": metrics.mean_conf_3 if metrics else 0.0,
        "sign_flip_count_5": metrics.sign_flip_count_5 if metrics else 0,
    }
    payload = StrategyOutputPayload(
        spec_version=c.SPEC_VERSION,
        instrument=bundle.instrument,
        asof_time=bundle.latest_payload.asof_time,
        strategy_family=strategy_family,
        contract_month_selection=contract_month_selection,
        final_strategy_strength=0 if strategy_family == "NO_TRADE" else final_strategy_strength,
        include_in_top_n=False,
        rank_overall=None,
        rank_in_family=None,
        strategy_transition_state=transition_state,
        reason_codes=list(dict.fromkeys(reason_codes)),
        input_snapshot=snapshot,
        errors=[{"code": err.code, "message": err.message} for err in errors],
    )
    validate_output_payload(payload)
    return payload


def validate_output_payload(payload: StrategyOutputPayload) -> None:
    if payload.spec_version != c.SPEC_VERSION:
        raise ValueError("invalid spec_version")
    if payload.strategy_family not in c.STRATEGY_FAMILIES:
        raise ValueError("invalid strategy_family")
    if payload.contract_month_selection not in c.CONTRACT_MONTHS:
        raise ValueError("invalid contract_month_selection")
    if payload.strategy_transition_state not in c.TRANSITION_STATES:
        raise ValueError("invalid strategy_transition_state")
    if payload.strategy_family == "NO_TRADE":
        if payload.contract_month_selection != "NO_CONTRACT_MONTH":
            raise ValueError("NO_TRADE must imply NO_CONTRACT_MONTH")
        if payload.final_strategy_strength != 0:
            raise ValueError("NO_TRADE must imply zero strength")
        if payload.rank_overall is not None or payload.rank_in_family is not None:
            raise ValueError("NO_TRADE must imply null ranks")
