from trend_identifier.schema import validate_payload


def test_schema_accepts_valid_payload():
    payload = {
        "spec_version": "v2.4",
        "instrument": "X",
        "asof_time": "2026-01-01T00:00:00+00:00",
        "label": "FLAT",
        "confidence": 0.2,
        "regime_strength": 0.0,
        "internal_state": "UNCLASSIFIABLE",
        "aggregate_score": None,
        "transition_state": "stable",
        "roll_flag": False,
        "data_quality_warning": False,
        "vetoes": {"shock": False, "volatility": False, "anomaly": False, "liquidity": False, "roll": False},
        "timeframes": {
            "weekly": {"label": "FLAT", "score": None, "direction": None, "quality": None, "noise": None},
            "daily": {"label": "FLAT", "score": None, "direction": None, "quality": None, "noise": None},
            "hourly": {"label": "FLAT", "score": None, "direction": None, "quality": None, "noise": None},
        },
        "diagnostics": {
            "acceptance_partial": False,
            "missing_bars_adjusted": False,
            "pivot_tie": False,
            "hourly_deterioration": False,
            "conflicts": {"major": False, "score": False, "quality": False},
            "reason_codes": [],
        },
        "errors": [],
    }
    validate_payload(payload)
