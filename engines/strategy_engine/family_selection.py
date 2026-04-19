"""Exact strategy-family rule evaluation."""
from __future__ import annotations

from .models import CandidateSelectionResult, HistoryMetrics


def select_candidate_family(metrics: HistoryMetrics) -> CandidateSelectionResult:
    """Evaluate rules A-F in locked order."""
    s_t = metrics.s_t
    c_t = metrics.c_t
    l_t = metrics.l_t

    if (l_t == "UP") and (s_t >= 40) and (c_t >= 0.60) and (metrics.bull_count_5 >= 3):
        return CandidateSelectionResult("BULL_CALL_SPREAD", ["RULE_STRONG_BULLISH_DIRECTIONAL"])

    if (l_t == "DOWN") and (s_t <= -40) and (c_t >= 0.60) and (metrics.bear_count_5 >= 3):
        return CandidateSelectionResult("BEAR_PUT_SPREAD", ["RULE_STRONG_BEARISH_DIRECTIONAL"])

    if (
        (l_t == "FLAT")
        and (abs(s_t) <= 10)
        and (c_t >= 0.50)
        and (metrics.flat_count_5 >= 4)
        and (abs(metrics.mean_score_3) <= 8)
        and (metrics.sign_flip_count_5 <= 1)
    ):
        return CandidateSelectionResult("IRON_CONDOR", ["RULE_TRUE_RANGE_PREMIUM_SELL"])

    if (
        (((l_t == "UP") and (10 < s_t < 40)) or ((l_t == "FLAT") and (10 <= s_t < 25)))
        and (c_t >= 0.45)
        and (metrics.bull_count_5 >= 3)
    ):
        return CandidateSelectionResult("BULL_PUT_SPREAD", ["RULE_MILD_BULLISH_CREDIT"])

    if (
        (((l_t == "DOWN") and (-40 < s_t < -10)) or ((l_t == "FLAT") and (-25 < s_t <= -10)))
        and (c_t >= 0.45)
        and (metrics.bear_count_5 >= 3)
    ):
        return CandidateSelectionResult("BEAR_CALL_SPREAD", ["RULE_MILD_BEARISH_CREDIT"])

    return CandidateSelectionResult("NO_TRADE", ["RULE_NO_TRADE_DEFAULT"])
