from trend_identifier.engine import evaluate_trend
from trend_identifier.enums import ReasonCode
from trend_identifier.types import PreviousState

from tests.utils import make_bars


def test_ut01_eligibility_fail_daily_bars_251():
    raw = {
        "weekly": make_bars(130, "W-FRI"),
        "daily": make_bars(251, "B"),
        "hourly": make_bars(700, "H"),
    }
    payload = evaluate_trend("X", raw["hourly"]["timestamp"].iloc[-1], "CAL", raw, {"instrument_type": "equity"})
    assert payload["internal_state"] == "UNCLASSIFIABLE"
    assert ReasonCode.GATE_HISTORY_DAILY.value in payload["diagnostics"]["reason_codes"]


def test_unclassifiable_public_output_behavior():
    raw = {
        "weekly": make_bars(100, "W-FRI"),
        "daily": make_bars(100, "B"),
        "hourly": make_bars(100, "H"),
    }
    payload = evaluate_trend("X", raw["hourly"]["timestamp"].iloc[-1], "CAL", raw, {"instrument_type": "equity"})
    assert payload["label"] == "FLAT"
    assert payload["aggregate_score"] is None
    assert payload["timeframes"]["weekly"]["score"] is None


def test_partial_breakout_acceptance_behavior_holds_schema_shape():
    raw = {
        "weekly": make_bars(130, "W-FRI", drift=0.8),
        "daily": make_bars(320, "B", drift=0.8),
        "hourly": make_bars(700, "H", drift=0.8),
    }
    payload = evaluate_trend("X", raw["hourly"]["timestamp"].iloc[-1], "CAL", raw, {"instrument_type": "equity"}, previous_state=PreviousState())
    assert payload["label"] in {"UP", "FLAT", "DOWN"}


def test_tie_handling_does_not_crash():
    raw = {
        "weekly": make_bars(130, "W-FRI"),
        "daily": make_bars(320, "B"),
        "hourly": make_bars(700, "H"),
    }
    # force equal highs for a tie window
    raw["daily"].loc[10:20, "high"] = raw["daily"].loc[10:20, "high"].iloc[0]
    payload = evaluate_trend("X", raw["hourly"]["timestamp"].iloc[-1], "CAL", raw, {"instrument_type": "equity"})
    assert payload["diagnostics"]["pivot_tie"] in {True, False}
