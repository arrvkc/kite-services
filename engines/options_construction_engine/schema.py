from __future__ import annotations

from typing import Any
from jsonschema import Draft202012Validator
from .constants import SPEC_IDENTIFIER, STATUS_CONSTRUCTED, STATUS_REJECTED, SUPPORTED_CONTRACT_MONTHS, SUPPORTED_FAMILIES

OUTPUT_SCHEMA: dict[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "title": "Options Construction Engine Output",
    "type": "object",
    "additionalProperties": False,
    "required": [
        "spec_identifier", "instrument", "asof_time", "strategy_family",
        "contract_month_selection", "construction_status", "execution_ready",
        "reason_codes", "errors",
    ],
    "properties": {
        "spec_identifier": {"const": SPEC_IDENTIFIER},
        "instrument": {"type": "string", "minLength": 1},
        "asof_time": {"type": "string", "format": "date-time"},
        "strategy_family": {"enum": SUPPORTED_FAMILIES},
        "contract_month_selection": {"enum": SUPPORTED_CONTRACT_MONTHS},
        "expiry": {"type": ["string", "null"], "format": "date"},
        "legs": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["role", "side", "option_type", "strike", "expiry", "tradingsymbol", "instrument_token", "bid_price", "ask_price"],
                "properties": {
                    "role": {"enum": ["LONG_LEG", "SHORT_LEG", "PUT_LONG_WING", "PUT_SHORT", "CALL_SHORT", "CALL_LONG_WING"]},
                    "side": {"enum": ["BUY", "SELL"]},
                    "option_type": {"enum": ["CE", "PE"]},
                    "strike": {"type": "number", "exclusiveMinimum": 0},
                    "expiry": {"type": "string", "format": "date"},
                    "tradingsymbol": {"type": "string", "minLength": 1},
                    "instrument_token": {"type": ["integer", "string"]},
                    "bid_price": {"type": ["number", "null"], "minimum": 0},
                    "ask_price": {"type": ["number", "null"], "minimum": 0},
                    "historical_reference_price": {"type": ["number", "null"], "minimum": 0},
                },
            },
        },
        "net_premium": {"type": ["number", "null"]},
        "max_loss_per_lot": {"type": ["number", "null"]},
        "max_profit_per_lot": {"type": ["number", "null"]},
        "breakeven_prices": {
            "type": "array",
            "items": {"type": "number", "exclusiveMinimum": 0},
            "minItems": 0,
            "maxItems": 2,
        },
        "construction_score": {"type": ["integer", "null"], "minimum": 0, "maximum": 100},
        "execution_ready": {"type": "boolean"},
        "construction_status": {"enum": [STATUS_CONSTRUCTED, STATUS_REJECTED]},
        "reason_codes": {"type": "array", "items": {"type": "string"}, "uniqueItems": True},
        "errors": {"type": "array", "items": {"type": "object", "required": ["code", "message"]}},
        "candidate_id": {"type": ["string", "null"], "pattern": "^[0-9a-f]{64}$"},
        "scored_candidates": {
            "type": "array",
            "items": {
                "type": "object",
                "required": ["candidate_id", "selected", "expiry", "legs", "net_premium", "width_value", "credit_or_debit_to_width", "max_loss_per_lot", "max_profit_per_lot", "breakeven_prices", "reward_risk_ratio", "roi", "construction_score"],
                "properties": {
                    "candidate_id": {"type": "string", "pattern": "^[0-9a-f]{64}$"},
                    "selected": {"type": "boolean"},
                    "expiry": {"type": "string", "format": "date"},
                    "legs": {"type": "array", "items": {"type": "object"}},
                    "net_premium": {"type": "number"},
                    "width_value": {"type": "number", "exclusiveMinimum": 0},
                    "credit_or_debit_to_width": {"type": ["number", "null"]},
                    "max_loss_per_lot": {"type": "number", "minimum": 0},
                    "max_profit_per_lot": {"type": "number", "minimum": 0},
                    "breakeven_prices": {"type": "array", "items": {"type": "number", "exclusiveMinimum": 0}, "maxItems": 2},
                    "reward_risk_ratio": {"type": ["number", "null"]},
                    "roi": {"type": ["number", "null"]},
                    "construction_score": {"type": "integer", "minimum": 0, "maximum": 100}
                },
                "additionalProperties": False
            }
        },
    },
}
VALIDATOR = Draft202012Validator(OUTPUT_SCHEMA)
