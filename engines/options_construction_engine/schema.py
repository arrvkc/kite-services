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
        "construction_score": {"type": ["integer", "null"], "minimum": 0, "maximum": 100},
        "execution_ready": {"type": "boolean"},
        "construction_status": {"enum": [STATUS_CONSTRUCTED, STATUS_REJECTED]},
        "reason_codes": {"type": "array", "items": {"type": "string"}, "uniqueItems": True},
        "errors": {"type": "array", "items": {"type": "object", "required": ["code", "message"]}},
    },
}
VALIDATOR = Draft202012Validator(OUTPUT_SCHEMA)
