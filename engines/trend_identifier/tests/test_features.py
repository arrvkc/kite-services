import pytest

from trend_identifier.data import load_and_align_data
from trend_identifier.features import compute_features
from trend_identifier.exceptions import MissingIntermediateError

from tests.utils import make_bars


def test_compute_features_basic():
    raw = {
        "weekly": make_bars(130, "W-FRI", seed=1),
        "daily": make_bars(320, "B", seed=2),
        "hourly": make_bars(700, "H", seed=3),
    }
    data = load_and_align_data("X", raw["hourly"]["timestamp"].iloc[-1], "CAL", raw, {"instrument_type": "equity"})
    feats = compute_features(data)
    assert feats.weekly.atr14 is not None
    assert feats.daily.volx is not None
    assert feats.hourly.eff is not None


def test_compute_features_missing_intermediate_raises():
    raw = {
        "weekly": make_bars(20, "W-FRI", seed=1),
        "daily": make_bars(50, "B", seed=2),
        "hourly": make_bars(80, "H", seed=3),
    }
    data = load_and_align_data("X", raw["hourly"]["timestamp"].iloc[-1], "CAL", raw, {"instrument_type": "equity"})
    with pytest.raises(MissingIntermediateError):
        compute_features(data)
