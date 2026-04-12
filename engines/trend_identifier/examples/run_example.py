"""Minimal example with synthetic data only."""

from __future__ import annotations

import numpy as np
import pandas as pd

from trend_identifier.engine import evaluate_trend
from trend_identifier.types import PreviousState


def make_bars(periods: int, freq: str, seed: int = 7) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    idx = pd.date_range("2024-01-01", periods=periods, freq=freq, tz="UTC")
    close = 100 + np.cumsum(rng.normal(0.2, 1.0, size=periods))
    open_ = close + rng.normal(0.0, 0.5, size=periods)
    high = np.maximum(open_, close) + rng.uniform(0.1, 1.0, size=periods)
    low = np.minimum(open_, close) - rng.uniform(0.1, 1.0, size=periods)
    volume = rng.integers(100_000, 200_000, size=periods)
    return pd.DataFrame({"timestamp": idx, "open": open_, "high": high, "low": low, "close": close, "volume": volume})


if __name__ == "__main__":
    raw = {
        "weekly": make_bars(130, "W-FRI", seed=1),
        "daily": make_bars(320, "B", seed=2),
        "hourly": make_bars(700, "H", seed=3),
    }
    payload = evaluate_trend(
        instrument="EXAMPLE",
        asof_time=raw["hourly"]["timestamp"].iloc[-1],
        calendar="EXAMPLE_CALENDAR",
        raw_bars=raw,
        instrument_metadata={"instrument_type": "equity"},
        previous_state=PreviousState(),
    )
    print(payload)
