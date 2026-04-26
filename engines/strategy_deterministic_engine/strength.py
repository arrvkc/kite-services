from __future__ import annotations

from .constants import (
    CONTRACT_NEAR_MONTH,
    FAMILY_BEAR_CALL_SPREAD,
    FAMILY_BEAR_PUT_SPREAD,
    FAMILY_BULL_CALL_SPREAD,
    FAMILY_BULL_PUT_SPREAD,
    FAMILY_IRON_CONDOR,
)
from .models import HistoryMetrics

def clip01(value: float) -> float:
    return min(max(value, 0.0), 1.0)

def compute_base_strategy_strength(candidate_family: str, aggregate_score: float, confidence: float, metrics: HistoryMetrics) -> int:
    instability_5 = metrics.instability_5
    if candidate_family == FAMILY_BULL_CALL_SPREAD:
        score_fit = clip01((aggregate_score - 40) / 20)
        conf_fit = clip01((confidence - 0.60) / 0.25)
        persist_fit = clip01((metrics.bull_count_5 - 2) / 3)
        return round(100 * clip01(0.50 * score_fit + 0.30 * conf_fit + 0.25 * persist_fit - 0.15 * instability_5))
    if candidate_family == FAMILY_BEAR_PUT_SPREAD:
        score_fit = clip01(((-aggregate_score) - 40) / 20)
        conf_fit = clip01((confidence - 0.60) / 0.25)
        persist_fit = clip01((metrics.bear_count_5 - 2) / 3)
        return round(100 * clip01(0.50 * score_fit + 0.30 * conf_fit + 0.25 * persist_fit - 0.15 * instability_5))
    if candidate_family == FAMILY_BULL_PUT_SPREAD:
        score_fit = clip01(1 - abs(aggregate_score - 25) / 20)
        conf_fit = clip01((confidence - 0.45) / 0.25)
        persist_fit = clip01((metrics.bull_count_5 - 2) / 3)
        return round(100 * clip01(0.45 * score_fit + 0.25 * conf_fit + 0.25 * persist_fit - 0.10 * instability_5))
    if candidate_family == FAMILY_BEAR_CALL_SPREAD:
        score_fit = clip01(1 - abs(aggregate_score + 25) / 20)
        conf_fit = clip01((confidence - 0.45) / 0.25)
        persist_fit = clip01((metrics.bear_count_5 - 2) / 3)
        return round(100 * clip01(0.45 * score_fit + 0.25 * conf_fit + 0.25 * persist_fit - 0.10 * instability_5))
    balance_fit = clip01(1 - abs(aggregate_score) / 10)
    conf_fit = clip01((confidence - 0.40) / 0.25)
    flat_fit = clip01((metrics.flat_count_5 - 3) / 2)
    drift_penalty = clip01(abs(metrics.mean_score_3) / 15)
    return round(100 * clip01(0.45 * balance_fit + 0.25 * conf_fit + 0.25 * flat_fit - 0.20 * drift_penalty))

def apply_contract_month_adjustment(base_strength: int, candidate_family: str, contract_month_selection: str) -> int:
    if contract_month_selection == CONTRACT_NEAR_MONTH:
        adjustment = 0
    elif candidate_family in {FAMILY_BULL_CALL_SPREAD, FAMILY_BEAR_PUT_SPREAD}:
        adjustment = -3
    elif candidate_family in {FAMILY_BULL_PUT_SPREAD, FAMILY_BEAR_CALL_SPREAD}:
        adjustment = -5
    else:
        adjustment = -8
    return max(base_strength + adjustment, 0)

def family_target_distance(strategy_family: str, aggregate_score: float) -> float:
    if strategy_family == FAMILY_BULL_CALL_SPREAD:
        return abs(aggregate_score - 50)
    if strategy_family == FAMILY_BEAR_PUT_SPREAD:
        return abs(aggregate_score + 50)
    if strategy_family == FAMILY_BULL_PUT_SPREAD:
        return abs(aggregate_score - 25)
    if strategy_family == FAMILY_BEAR_CALL_SPREAD:
        return abs(aggregate_score + 25)
    return abs(aggregate_score)
