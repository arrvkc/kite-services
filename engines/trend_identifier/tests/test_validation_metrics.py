import pandas as pd

from trend_identifier.validation import (
    chop_capture_share,
    direct_reversal_fraction,
    flat_miss_rate,
    label_persistence,
    turnover,
)


def test_validation_metrics():
    labels = pd.Series(["FLAT", "UP", "UP", "FLAT", "DOWN", "DOWN", "FLAT"])
    assert turnover(labels) > 0
    assert direct_reversal_fraction(labels) == 0.0
    persistence = label_persistence(labels)
    assert persistence["UP"] == 2.0
    assert chop_capture_share(labels, pd.Series([True] * len(labels))) > 0
    assert flat_miss_rate(labels, pd.Series([0, 0, 0, 3, 0, 0, 3]), pd.Series([0, 0, 0, 0.5, 0, 0, 0.5])) > 0
