from trend_identifier.hysteresis import apply_hysteresis
from trend_identifier.types import PreviousState, ScoreBundle, TimeframeScore, VetoFlags


def make_scores(agg, weekly, daily):
    return ScoreBundle(
        weekly=TimeframeScore("UP" if weekly > 0 else "DOWN", weekly, 0.0, 0.2, 0.1),
        daily=TimeframeScore("UP" if daily > 0 else "DOWN", daily, 0.0, 0.2, 0.1),
        hourly=TimeframeScore("FLAT", 0.0, 0.0, 0.1, 0.1),
        aggregate_score=agg,
    )


def test_fast_track_flat_to_up():
    prev = PreviousState(label="FLAT")
    label, transition, _, _ = apply_hysteresis("UP", prev, make_scores(42.0, 27.0, 20.0), VetoFlags())
    assert label == "UP"
    assert transition == "fast_track"


def test_direct_reversal_blocked():
    prev = PreviousState(label="UP")
    label, transition, _, reasons = apply_hysteresis("DOWN", prev, make_scores(-30.0, -26.0, -26.0), VetoFlags())
    assert label == "FLAT"
    assert transition == "forced_flat"
