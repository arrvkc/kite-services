from __future__ import annotations

import copy
from typing import Any

from jsonschema import Draft202012Validator

from .constants import (
    CONTRACT_NEAR_MONTH,
    CONTRACT_NEXT_MONTH,
    REASON_RUNTIME_SCHEMA_VALIDATION_FAILED,
    SPEC_VERSION,
    SUPPORTED_FAMILIES,
    TRANSITION_ENUM,
)
from .contract_month import select_contract_month
from .family_selection import classify_regime, select_candidate_family
from .hysteresis import apply_strategy_hysteresis
from .metrics import compute_history_metrics
from .ranking import rank_batch
from .strength import apply_contract_month_adjustment, compute_base_strategy_strength
from .validators import validate_inputs

OUTPUT_SCHEMA: dict[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$id": "https://example.internal/schemas/strategy-engine-v2.0.json",
    "title": "Strategy Engine v2.0 Output",
    "type": "object",
    "additionalProperties": False,
    "required": [
        "spec_version",
        "instrument",
        "asof_time",
        "strategy_family",
        "contract_month_selection",
        "final_strategy_strength",
        "include_in_top_n",
        "rank_overall",
        "rank_in_family",
        "strategy_transition_state",
        "reason_codes",
        "input_snapshot",
        "errors",
    ],
    "properties": {
        "spec_version": {"const": "strategy-engine-v2.0"},
        "instrument": {"type": "string", "minLength": 1},
        "asof_time": {"type": "string", "format": "date-time"},
        "strategy_family": {"enum": SUPPORTED_FAMILIES},
        "contract_month_selection": {"enum": [CONTRACT_NEAR_MONTH, CONTRACT_NEXT_MONTH]},
        "final_strategy_strength": {"type": "integer", "minimum": 0, "maximum": 100},
        "include_in_top_n": {"type": "boolean"},
        "rank_overall": {"type": ["integer", "null"], "minimum": 1},
        "rank_in_family": {"type": ["integer", "null"], "minimum": 1},
        "strategy_transition_state": {"enum": TRANSITION_ENUM},
        "reason_codes": {"type": "array", "items": {"type": "string", "minLength": 1}, "uniqueItems": True},
        "input_snapshot": {
            "type": "object",
            "additionalProperties": False,
            "required": [
                "label",
                "confidence",
                "aggregate_score",
                "dte_near_month",
                "next_month_available",
                "dte_next_month",
                "bull_count_5",
                "bear_count_5",
                "flat_count_5",
                "mean_score_3",
                "mean_conf_3",
                "sign_flip_count_5",
            ],
            "properties": {
                "label": {"enum": ["UP", "FLAT", "DOWN"]},
                "confidence": {"type": "number", "minimum": 0.0, "maximum": 1.0},
                "aggregate_score": {"type": "number"},
                "dte_near_month": {"type": "integer", "minimum": 0},
                "next_month_available": {"type": "boolean"},
                "dte_next_month": {"type": ["integer", "null"], "minimum": 0},
                "bull_count_5": {"type": "integer", "minimum": 0, "maximum": 5},
                "bear_count_5": {"type": "integer", "minimum": 0, "maximum": 5},
                "flat_count_5": {"type": "integer", "minimum": 0, "maximum": 5},
                "mean_score_3": {"type": "number"},
                "mean_conf_3": {"type": "number", "minimum": 0.0, "maximum": 1.0},
                "sign_flip_count_5": {"type": "integer", "minimum": 0, "maximum": 4},
            },
            "allOf": [
                {
                    "if": {"properties": {"next_month_available": {"const": True}}},
                    "then": {"properties": {"dte_next_month": {"type": "integer", "minimum": 0}}},
                    "else": {"properties": {"dte_next_month": {"type": "null"}}},
                }
            ],
        },
        "errors": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["code", "message"],
                "properties": {
                    "code": {"type": "string", "minLength": 1},
                    "message": {"type": "string", "minLength": 1},
                },
            },
        },
    },
    "allOf": [
        {
            "if": {"properties": {"include_in_top_n": {"const": True}}},
            "then": {
                "properties": {
                    "rank_overall": {"type": "integer", "minimum": 1},
                    "rank_in_family": {"type": "integer", "minimum": 1},
                }
            },
        }
    ],
}

_VALIDATOR = Draft202012Validator(OUTPUT_SCHEMA)


def _error(code: str, message: str) -> dict[str, str]:
    return {"code": code, "message": message}


def _strategy_input_to_payload(item: Any) -> dict[str, Any]:
    if isinstance(item, dict):
        return item

    latest = item.latest_payload
    return {
        "instrument": item.instrument,
        "asof_time": latest.asof_time,
        "label": latest.label,
        "confidence": latest.confidence,
        "aggregate_score": latest.aggregate_score,
        "internal_state": latest.internal_state,
        "trend_history_w5": [
            {
                "label": row.label,
                "confidence": row.confidence,
                "aggregate_score": row.aggregate_score,
            }
            for row in item.trend_history_w5
        ],
        "dte_near_month": item.dte_near_month,
        "next_month_available": item.next_month_available,
        "dte_next_month": item.dte_next_month,
        "in_universe": item.in_universe,
        "prior_committed_state": item.prior_committed_state,
    }


def public_output(
    instrument: str,
    asof_time: str,
    strategy_family: str,
    contract_month_selection: str,
    final_strategy_strength: int,
    strategy_transition_state: str,
    reason_codes: list[str],
    label: str,
    confidence: float,
    aggregate_score: float,
    dte_near_month: int,
    next_month_available: bool,
    dte_next_month: int | None,
    metrics,
) -> dict[str, Any]:
    payload = {
        "spec_version": SPEC_VERSION,
        "instrument": instrument,
        "asof_time": asof_time,
        "strategy_family": strategy_family,
        "contract_month_selection": contract_month_selection,
        "final_strategy_strength": int(final_strategy_strength),
        "include_in_top_n": False,
        "rank_overall": None,
        "rank_in_family": None,
        "strategy_transition_state": strategy_transition_state,
        "reason_codes": list(dict.fromkeys(reason_codes)),
        "input_snapshot": {
            "label": label,
            "confidence": confidence,
            "aggregate_score": aggregate_score,
            "dte_near_month": dte_near_month,
            "next_month_available": next_month_available,
            "dte_next_month": dte_next_month if next_month_available else None,
            "bull_count_5": metrics.bull_count_5,
            "bear_count_5": metrics.bear_count_5,
            "flat_count_5": metrics.flat_count_5,
            "mean_score_3": metrics.mean_score_3,
            "mean_conf_3": metrics.mean_conf_3,
            "sign_flip_count_5": metrics.sign_flip_count_5,
        },
        "errors": [],
    }
    return payload


def evaluate_strategy_engine(payload: dict[str, Any]) -> dict[str, Any]:
    validated, invalid_record = validate_inputs(payload)
    if invalid_record is not None:
        return {
            "mode": "invalid_evaluation",
            "invalid_evaluation_record": {
                "instrument": invalid_record.instrument,
                "asof_time": invalid_record.asof_time,
                "reason_codes": invalid_record.reason_codes,
                "errors": invalid_record.errors,
                "input_snapshot": invalid_record.input_snapshot,
            },
        }

    metrics = compute_history_metrics(validated.trend_history_w5)
    regime_bucket = classify_regime(validated.label, validated.aggregate_score, metrics)
    candidate_family, family_reasons = select_candidate_family(
        regime_bucket,
        validated.aggregate_score,
        validated.confidence,
        metrics,
    )
    contract_month_selection, month_reasons = select_contract_month(
        candidate_family,
        validated.dte_near_month,
        validated.dte_next_month,
        validated.next_month_available,
    )
    base_strategy_strength = compute_base_strategy_strength(
        candidate_family,
        validated.aggregate_score,
        validated.confidence,
        metrics,
    )
    final_strategy_strength = apply_contract_month_adjustment(
        base_strategy_strength,
        candidate_family,
        contract_month_selection,
    )
    final_family, final_month, transition_state, transition_reasons, next_state = apply_strategy_hysteresis(
        candidate_family,
        contract_month_selection,
        validated.prior_committed_state,
        final_strategy_strength,
        validated.aggregate_score,
    )

    payload_out = public_output(
        instrument=validated.instrument,
        asof_time=validated.asof_time,
        strategy_family=final_family,
        contract_month_selection=final_month,
        final_strategy_strength=final_strategy_strength,
        strategy_transition_state=transition_state,
        reason_codes=family_reasons + month_reasons + transition_reasons,
        label=validated.label,
        confidence=validated.confidence,
        aggregate_score=validated.aggregate_score,
        dte_near_month=validated.dte_near_month,
        next_month_available=validated.next_month_available,
        dte_next_month=validated.dte_next_month,
        metrics=metrics,
    )

    schema_errors = sorted(_VALIDATOR.iter_errors(payload_out), key=lambda error: error.path)
    if schema_errors:
        return {
            "mode": "runtime_blocked",
            "payload": payload_out,
            "runtime_errors": [
                _error(
                    REASON_RUNTIME_SCHEMA_VALIDATION_FAILED,
                    "; ".join(error.message for error in schema_errors),
                )
            ],
        }

    return {
        "mode": "public_payload",
        "payload": payload_out,
        "next_prior_committed_state": {
            "instrument": validated.instrument,
            "asof_time": validated.asof_time,
            "strategy_family": next_state["strategy_family"],
            "contract_month_selection": next_state["contract_month_selection"],
            "strategy_transition_state": next_state["strategy_transition_state"],
            "pending_counter": next_state["pending_counter"],
            "pending_candidate_family": next_state["pending_candidate_family"],
            "pending_candidate_month": next_state["pending_candidate_month"],
            "state_version": SPEC_VERSION,
        },
    }


def evaluate_batch(items) -> dict[str, Any]:
    results = []
    published = []

    for item in items:
        payload = _strategy_input_to_payload(item)
        result = evaluate_strategy_engine(payload)
        results.append(result)
        if result["mode"] == "public_payload":
            published.append(copy.deepcopy(result["payload"]))

    ranked_payloads = rank_batch(published)
    ranked_map = {(payload["instrument"], payload["asof_time"]): payload for payload in ranked_payloads}

    final_results = []
    for result in results:
        if result["mode"] == "public_payload":
            key = (result["payload"]["instrument"], result["payload"]["asof_time"])
            result["payload"] = ranked_map[key]
        final_results.append(result)

    return {
        "spec_version": SPEC_VERSION,
        "batch_metadata": {
            "ranking_mode": "overall_and_family",
            "total_inputs": len(items),
            "published_count": len(published),
            "invalid_count": len([item for item in results if item["mode"] != "public_payload"]),
        },
        "results": final_results,
    }
