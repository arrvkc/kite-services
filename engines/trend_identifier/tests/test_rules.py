from trend_identifier.types import ScoreBundle, TimeframeScore, VetoFlags, ConflictFlags
from trend_identifier.rules import assign_provisional_labels, apply_aggregation_rules


def test_assign_provisional_labels_up():
    scores = ScoreBundle(
        weekly=TimeframeScore("FLAT", 30.0, 0.5, 0.3, 0.1),
        daily=TimeframeScore("FLAT", 20.0, 0.4, 0.2, 0.1),
        hourly=TimeframeScore("FLAT", 10.0, 0.2, 0.1, 0.1),
        aggregate_score=23.5,
    )
    out = assign_provisional_labels(scores)
    assert out.weekly.label == "UP"
    assert out.daily.label == "UP"


def test_apply_aggregation_rules_conflict_forces_flat():
    scores = ScoreBundle(
        weekly=TimeframeScore("UP", 30.0, 0.5, 0.3, 0.1),
        daily=TimeframeScore("DOWN", -20.0, -0.4, 0.2, 0.1),
        hourly=TimeframeScore("FLAT", 0.0, 0.0, 0.1, 0.1),
        aggregate_score=10.0,
    )
    conflicts = ConflictFlags(major=True, score=False, quality=False)
    label = apply_aggregation_rules(scores, scores, VetoFlags(), conflicts)
    assert label == "FLAT"
