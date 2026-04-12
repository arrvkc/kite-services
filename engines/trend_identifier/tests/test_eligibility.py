from trend_identifier.data import load_and_align_data
from trend_identifier.eligibility import passes_eligibility_gate
from trend_identifier.enums import ReasonCode

from tests.utils import make_bars


def test_eligibility_fails_daily_history():
    raw = {
        "weekly": make_bars(130, "W-FRI"),
        "daily": make_bars(251, "B"),
        "hourly": make_bars(700, "H"),
    }
    data = load_and_align_data("X", raw["hourly"]["timestamp"].iloc[-1], "CAL", raw, {"instrument_type": "equity"})
    gate = passes_eligibility_gate(data)
    assert gate.passed is False
    assert ReasonCode.GATE_HISTORY_DAILY.value in gate.reason_codes


def test_eligibility_passes_for_sufficient_data():
    raw = {
        "weekly": make_bars(130, "W-FRI"),
        "daily": make_bars(300, "B"),
        "hourly": make_bars(700, "H"),
    }
    data = load_and_align_data("X", raw["hourly"]["timestamp"].iloc[-1], "CAL", raw, {"instrument_type": "equity"})
    gate = passes_eligibility_gate(data)
    assert gate.passed is True
