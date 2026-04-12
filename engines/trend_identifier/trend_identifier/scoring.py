"""Score construction."""

from __future__ import annotations

from typing import Dict

from .constants import CONSTANTS
from .enums import PublicLabel
from .types import FeatureBundle, ScoreBundle, TimeframeScore


def zclip(x: float) -> float:
    # SPEC TRACE: Section 6 - clip exactly as specified
    return min(max(float(x), -1.0), 1.0)


def _score_one(feature, timeframe: str) -> TimeframeScore:
    # SPEC TRACE: Section 6 - exact score construction
    c = CONSTANTS.timeframes[timeframe]
    direction = (
        0.35 * zclip(feature.nr / c.direction_k_nr) +
        0.25 * zclip(feature.bs / c.direction_k_bs) +
        0.20 * zclip(feature.sep / c.direction_k_sep) +
        0.10 * int(feature.sw) +
        0.10 * int(feature.breakout_sign)
    )
    quality = (
        0.50 * zclip((feature.eff - c.eff_min) / (1 - c.eff_min)) +
        0.30 * zclip((feature.acc - c.acc_min) / (1 - c.acc_min)) -
        0.20 * feature.ovl
    )
    noise = (
        0.35 * zclip(feature.gap / c.gap_veto) +
        0.35 * zclip((feature.volx - 1) / (c.volx_veto - 1)) +
        0.20 * feature.anom +
        0.10 * feature.liq
    )
    score = 100.0 * (0.55 * direction + 0.30 * quality - 0.15 * noise)
    label = PublicLabel.FLAT.value
    return TimeframeScore(label=label, score=float(score), direction=float(direction), quality=float(quality), noise=float(noise))


def compute_timeframe_scores(features: FeatureBundle) -> ScoreBundle:
    weekly = _score_one(features.weekly, "weekly")
    daily = _score_one(features.daily, "daily")
    hourly = _score_one(features.hourly, "hourly")
    hourly_weight = CONSTANTS.timeframes["hourly"].weight
    aggregate = (
        CONSTANTS.timeframes["weekly"].weight * weekly.score +
        CONSTANTS.timeframes["daily"].weight * daily.score +
        hourly_weight * hourly.score
    )
    return ScoreBundle(weekly=weekly, daily=daily, hourly=hourly, aggregate_score=float(aggregate))
