"""JSON Schema and validation for Trend Identifier payloads."""

from typing import Any, Dict

from jsonschema import Draft202012Validator

from .exceptions import SchemaValidationError

# SPEC TRACE: Section 11 - formal JSON Schema and public payload policy
TREND_IDENTIFIER_SCHEMA: Dict[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$id": "https://example.internal/schemas/trend-identifier-v2.4.json",
    "title": "Trend Identifier v2.4 Output",
    "type": "object",
    "additionalProperties": False,
    "required": [
        "spec_version",
        "instrument",
        "asof_time",
        "label",
        "confidence",
        "regime_strength",
        "internal_state",
        "aggregate_score",
        "transition_state",
        "roll_flag",
        "data_quality_warning",
        "vetoes",
        "timeframes",
        "diagnostics",
        "errors",
    ],
    "properties": {
        "spec_version": {"const": "v2.4"},
        "instrument": {"type": "string", "minLength": 1},
        "asof_time": {"type": "string", "format": "date-time"},
        "label": {"enum": ["UP", "FLAT", "DOWN"]},
        "confidence": {"type": "number", "minimum": 0.0, "maximum": 1.0},
        "regime_strength": {"type": "number", "minimum": 0.0, "maximum": 1.0},
        "internal_state": {"enum": ["CLASSIFIABLE", "UNCLASSIFIABLE"]},
        "aggregate_score": {"type": ["number", "null"]},
        "transition_state": {
            "enum": ["stable", "pending_upgrade", "pending_downgrade", "forced_flat", "fast_track"]
        },
        "roll_flag": {"type": "boolean"},
        "data_quality_warning": {"type": "boolean"},
        "vetoes": {"$ref": "#/$defs/vetoes"},
        "timeframes": {"$ref": "#/$defs/timeframes"},
        "diagnostics": {"$ref": "#/$defs/diagnostics"},
        "errors": {
            "type": "array",
            "items": {"$ref": "#/$defs/error"},
        },
    },
    "allOf": [
        {
            "if": {"properties": {"internal_state": {"const": "UNCLASSIFIABLE"}}},
            "then": {
                "properties": {
                    "label": {"const": "FLAT"},
                    "confidence": {"maximum": 0.2},
                    "regime_strength": {"const": 0.0},
                    "aggregate_score": {"type": "null"},
                    "timeframes": {"$ref": "#/$defs/timeframes_unclassifiable"},
                }
            },
            "else": {
                "properties": {
                    "aggregate_score": {"type": "number"},
                    "timeframes": {"$ref": "#/$defs/timeframes_classifiable"},
                }
            },
        }
    ],
    "$defs": {
        "score_block": {
            "type": "object",
            "additionalProperties": False,
            "required": ["label", "score", "direction", "quality", "noise"],
            "properties": {
                "label": {"enum": ["UP", "FLAT", "DOWN"]},
                "score": {"type": ["number", "null"]},
                "direction": {"type": ["number", "null"]},
                "quality": {"type": ["number", "null"]},
                "noise": {"type": ["number", "null"]},
            },
        },
        "score_block_nonnull": {
            "type": "object",
            "additionalProperties": False,
            "required": ["label", "score", "direction", "quality", "noise"],
            "properties": {
                "label": {"enum": ["UP", "FLAT", "DOWN"]},
                "score": {"type": "number"},
                "direction": {"type": "number"},
                "quality": {"type": "number"},
                "noise": {"type": "number"},
            },
        },
        "timeframes": {
            "type": "object",
            "additionalProperties": False,
            "required": ["weekly", "daily", "hourly"],
            "properties": {
                "weekly": {"$ref": "#/$defs/score_block"},
                "daily": {"$ref": "#/$defs/score_block"},
                "hourly": {"$ref": "#/$defs/score_block"},
            },
        },
        "timeframes_classifiable": {
            "type": "object",
            "additionalProperties": False,
            "required": ["weekly", "daily", "hourly"],
            "properties": {
                "weekly": {"$ref": "#/$defs/score_block_nonnull"},
                "daily": {"$ref": "#/$defs/score_block_nonnull"},
                "hourly": {"$ref": "#/$defs/score_block_nonnull"},
            },
        },
        "timeframes_unclassifiable": {
            "type": "object",
            "additionalProperties": False,
            "required": ["weekly", "daily", "hourly"],
            "properties": {
                "weekly": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["label", "score", "direction", "quality", "noise"],
                    "properties": {
                        "label": {"const": "FLAT"},
                        "score": {"type": "null"},
                        "direction": {"type": "null"},
                        "quality": {"type": "null"},
                        "noise": {"type": "null"},
                    },
                },
                "daily": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["label", "score", "direction", "quality", "noise"],
                    "properties": {
                        "label": {"const": "FLAT"},
                        "score": {"type": "null"},
                        "direction": {"type": "null"},
                        "quality": {"type": "null"},
                        "noise": {"type": "null"},
                    },
                },
                "hourly": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["label", "score", "direction", "quality", "noise"],
                    "properties": {
                        "label": {"const": "FLAT"},
                        "score": {"type": "null"},
                        "direction": {"type": "null"},
                        "quality": {"type": "null"},
                        "noise": {"type": "null"},
                    },
                },
            },
        },
        "vetoes": {
            "type": "object",
            "additionalProperties": False,
            "required": ["shock", "volatility", "anomaly", "liquidity", "roll"],
            "properties": {
                "shock": {"type": "boolean"},
                "volatility": {"type": "boolean"},
                "anomaly": {"type": "boolean"},
                "liquidity": {"type": "boolean"},
                "roll": {"type": "boolean"},
            },
        },
        "diagnostics": {
            "type": "object",
            "additionalProperties": False,
            "required": [
                "acceptance_partial",
                "missing_bars_adjusted",
                "pivot_tie",
                "hourly_deterioration",
                "conflicts",
                "reason_codes",
            ],
            "properties": {
                "acceptance_partial": {"type": "boolean"},
                "missing_bars_adjusted": {"type": "boolean"},
                "pivot_tie": {"type": "boolean"},
                "hourly_deterioration": {"type": "boolean"},
                "conflicts": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["major", "score", "quality"],
                    "properties": {
                        "major": {"type": "boolean"},
                        "score": {"type": "boolean"},
                        "quality": {"type": "boolean"},
                    },
                },
                "reason_codes": {
                    "type": "array",
                    "items": {
                        "enum": [
                            "GATE_HISTORY_WEEKLY",
                            "GATE_HISTORY_DAILY",
                            "GATE_HISTORY_HOURLY",
                            "GATE_MISSING_BARS",
                            "GATE_LIQUIDITY",
                            "GATE_BAD_CANDLE",
                            "GATE_STALE_DATA",
                            "NUMERIC_INVALID",
                            "MISSING_INTERMEDIATE_LOAD_BEARING",
                            "REALIZED_VOL_INSUFFICIENT",
                            "ACCEPTANCE_PARTIAL",
                            "DIAGNOSTIC_MISSING",
                            "CONFLICT_MAJOR",
                            "CONFLICT_SCORE",
                            "CONFLICT_QUALITY",
                            "VETO_SHOCK",
                            "VETO_VOLATILITY",
                            "VETO_ANOMALY",
                            "VETO_LIQUIDITY",
                            "VETO_ROLL",
                            "TRANSITION_PENDING_UPGRADE",
                            "TRANSITION_PENDING_DOWNGRADE",
                            "TRANSITION_FORCED_FLAT",
                            "RUNTIME_SCHEMA_VALIDATION_FAILED",
                            "RUNTIME_STATE_PERSIST_FAILED",
                            "RUNTIME_LOG_WRITE_FAILED",
                            "RUNTIME_PREVIOUS_STATE_LOCK_FAILED",
                        ]
                    },
                    "uniqueItems": True,
                },
            },
        },
        "error": {
            "type": "object",
            "additionalProperties": False,
            "required": ["code", "message"],
            "properties": {
                "code": {"type": "string", "minLength": 1},
                "message": {"type": "string", "minLength": 1},
            },
        },
    },
}


def validate_payload(payload: Dict[str, Any]) -> None:
    """Validate a payload against the normative schema."""
    validator = Draft202012Validator(TREND_IDENTIFIER_SCHEMA)
    errors = sorted(validator.iter_errors(payload), key=lambda e: list(e.path))
    if errors:
        message = "; ".join(error.message for error in errors)
        raise SchemaValidationError(message)
