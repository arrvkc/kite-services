"""Rule logic, vetoes, and conflicts."""

from __future__ import annotations

from .constants import CONSTANTS
from .enums import PublicLabel, ReasonCode
from .types import ConflictFlags, FeatureBundle, ScoreBundle, VetoFlags


def assign_provisional_labels(scores: ScoreBundle) -> ScoreBundle:
    # SPEC TRACE: Section 7 - parenthesized rule logic
    for timeframe in ("weekly", "daily", "hourly"):
        score_block = getattr(scores, timeframe)
        c = CONSTANTS.timeframes[timeframe]
        if (score_block.score >= c.up_threshold) and (score_block.quality >= c.quality_min):
            score_block.label = PublicLabel.UP.value
        elif (score_block.score <= c.down_threshold) and (score_block.quality >= c.quality_min):
            score_block.label = PublicLabel.DOWN.value
        else:
            score_block.label = PublicLabel.FLAT.value
    return scores


def compute_veto_flags(features: FeatureBundle, provisional: ScoreBundle, scores: ScoreBundle, roll_flag: bool = False) -> VetoFlags:
    # SPEC TRACE: Section 7 - exact veto logic
    shock_veto = (((features.daily.gap >= 2.0) or (features.weekly.gap >= 2.0)) and (features.daily.acc < 0.60))
    volatility_veto = ((features.daily.volx >= 1.75) and (features.daily.eff < 0.30))
    anomaly_veto = ((features.daily.anom == 1) and (features.hourly.anom == 1))
    liquidity_veto = ((features.daily.liq == 1) or (features.hourly.liq == 1))
    return VetoFlags(
        shock=bool(shock_veto),
        volatility=bool(volatility_veto),
        anomaly=bool(anomaly_veto),
        liquidity=bool(liquidity_veto),
        roll=bool(roll_flag),
    )


def compute_conflicts(provisional: ScoreBundle, scores: ScoreBundle) -> ConflictFlags:
    major_conflict = ((provisional.weekly.label == "UP") and (provisional.daily.label == "DOWN")) or ((provisional.weekly.label == "DOWN") and (provisional.daily.label == "UP"))
    score_conflict = ((scores.weekly.score > 0) != (scores.daily.score > 0)) and (abs(scores.weekly.score) >= 12) and (abs(scores.daily.score) >= 12)
    quality_conflict = (
        (provisional.daily.label == provisional.weekly.label) and
        (scores.daily.quality < 0) and
        (((scores.hourly.score <= -18) and (provisional.weekly.label == "UP")) or ((scores.hourly.score >= 18) and (provisional.weekly.label == "DOWN")))
    )
    return ConflictFlags(major=bool(major_conflict), score=bool(score_conflict), quality=bool(quality_conflict))


def apply_aggregation_rules(provisional: ScoreBundle, scores: ScoreBundle, vetoes: VetoFlags, conflicts: ConflictFlags) -> str:
    # SPEC TRACE: Section 7 - rules in documented order only
    any_veto_flag = vetoes.shock or vetoes.volatility or vetoes.anomaly or vetoes.liquidity or vetoes.roll
    weekly_label = provisional.weekly.label
    daily_label = provisional.daily.label
    hourly_label = provisional.hourly.label
    weekly_score = scores.weekly.score
    daily_score = scores.daily.score
    hourly_score = scores.hourly.score
    aggregate_score = scores.aggregate_score

    if (weekly_label == "FLAT") and not (((weekly_score >= 18) and (daily_score >= 10)) or ((weekly_score <= -18) and (daily_score <= -10))):
        return PublicLabel.FLAT.value
    elif (conflicts.major is True) or (conflicts.score is True):
        return PublicLabel.FLAT.value
    elif (any_veto_flag is True) and (abs(aggregate_score) < 35):
        return PublicLabel.FLAT.value
    elif (weekly_label == "UP") and (daily_label == "UP") and not ((hourly_label == "DOWN") and (hourly_score <= -20)):
        return PublicLabel.UP.value
    elif (weekly_label == "DOWN") and (daily_label == "DOWN") and not ((hourly_label == "UP") and (hourly_score >= 20)):
        return PublicLabel.DOWN.value
    elif aggregate_score >= 25:
        return PublicLabel.UP.value
    elif aggregate_score <= -25:
        return PublicLabel.DOWN.value
    else:
        return PublicLabel.FLAT.value
