from trend_identifier.data import load_and_align_data
from trend_identifier.features import compute_features
from trend_identifier.scoring import compute_timeframe_scores

from tests.utils import make_bars


def test_scoring_produces_aggregate():
    raw = {
        "weekly": make_bars(130, "W-FRI"),
        "daily": make_bars(320, "B"),
        "hourly": make_bars(700, "H"),
    }
    data = load_and_align_data("X", raw["hourly"]["timestamp"].iloc[-1], "CAL", raw, {"instrument_type": "equity"})
    feats = compute_features(data)
    scores = compute_timeframe_scores(feats)
    assert isinstance(scores.aggregate_score, float)
    assert scores.weekly.score is not None
