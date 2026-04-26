from __future__ import annotations

from .constants import (
    FAST_TRACK_STRENGTH,
    FAMILY_BEAR_CALL_SPREAD,
    FAMILY_BEAR_PUT_SPREAD,
    FAMILY_BULL_CALL_SPREAD,
    FAMILY_BULL_PUT_SPREAD,
    FAMILY_IRON_CONDOR,
    HIGH_CONFIDENCE,
    LABEL_DOWN,
    LABEL_FLAT,
    LABEL_UP,
    REGIME_BEARISH,
    REGIME_BULLISH,
    REGIME_FLAT,
    REASON_RULE_MANDATORY_BEARISH_FALLBACK,
    REASON_RULE_MANDATORY_BULLISH_FALLBACK,
    REASON_RULE_MANDATORY_FLAT_MAPPING,
    REASON_RULE_STRONG_BEARISH_DIRECTIONAL,
    REASON_RULE_STRONG_BULLISH_DIRECTIONAL,
    STRONG_BEAR_SCORE,
    STRONG_BULL_SCORE,
)
from .models import HistoryMetrics

def classify_regime(label: str, aggregate_score: float, metrics: HistoryMetrics) -> str:
    if label == LABEL_UP and aggregate_score >= 10 and metrics.bull_count_5 >= 3:
        return REGIME_BULLISH
    if label == LABEL_DOWN and aggregate_score <= -10 and metrics.bear_count_5 >= 3:
        return REGIME_BEARISH
    if label == LABEL_FLAT and aggregate_score >= 10 and metrics.bull_count_5 >= 3:
        return REGIME_BULLISH
    if label == LABEL_FLAT and aggregate_score <= -10 and metrics.bear_count_5 >= 3:
        return REGIME_BEARISH
    return REGIME_FLAT

def select_candidate_family(regime_bucket: str, aggregate_score: float, confidence: float, metrics: HistoryMetrics) -> tuple[str, list[str]]:
    if regime_bucket == REGIME_BULLISH and aggregate_score >= STRONG_BULL_SCORE and confidence >= HIGH_CONFIDENCE and metrics.bull_count_5 >= 3:
        return FAMILY_BULL_CALL_SPREAD, [REASON_RULE_STRONG_BULLISH_DIRECTIONAL]
    if regime_bucket == REGIME_BEARISH and aggregate_score <= STRONG_BEAR_SCORE and confidence >= HIGH_CONFIDENCE and metrics.bear_count_5 >= 3:
        return FAMILY_BEAR_PUT_SPREAD, [REASON_RULE_STRONG_BEARISH_DIRECTIONAL]
    if regime_bucket == REGIME_BULLISH:
        return FAMILY_BULL_PUT_SPREAD, [REASON_RULE_MANDATORY_BULLISH_FALLBACK]
    if regime_bucket == REGIME_BEARISH:
        return FAMILY_BEAR_CALL_SPREAD, [REASON_RULE_MANDATORY_BEARISH_FALLBACK]
    return FAMILY_IRON_CONDOR, [REASON_RULE_MANDATORY_FLAT_MAPPING]
