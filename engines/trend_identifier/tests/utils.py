import numpy as np
import pandas as pd


def _normalized_freq_and_end(freq: str) -> tuple[str, str]:
    if freq == "H":
        return "h", "2026-01-30 23:00:00"
    if freq == "h":
        return "h", "2026-01-30 23:00:00"
    if freq == "W-FRI":
        return "W-FRI", "2026-01-30"
    if freq == "B":
        return "B", "2026-01-30"
    return freq, "2026-01-30"


def make_bars(periods, freq, seed=1, drift=0.2, volume_low=100_000, volume_high=200_000):
    rng = np.random.default_rng(seed)

    freq_normalized, end_value = _normalized_freq_and_end(freq)
    idx = pd.date_range(end=end_value, periods=periods, freq=freq_normalized, tz="UTC")

    close = 100 + np.cumsum(rng.normal(drift, 0.8, size=periods))
    open_ = close + rng.normal(0.0, 0.3, size=periods)
    high = np.maximum(open_, close) + rng.uniform(0.1, 0.8, size=periods)
    low = np.minimum(open_, close) - rng.uniform(0.1, 0.8, size=periods)
    volume = rng.integers(volume_low, volume_high, size=periods)

    return pd.DataFrame(
        {
            "timestamp": idx,
            "open": open_,
            "high": high,
            "low": low,
            "close": close,
            "volume": volume,
        }
    )
