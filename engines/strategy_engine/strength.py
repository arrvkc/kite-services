"""Strategy strength scoring."""
from __future__ import annotations

from .models import HistoryMetrics


def clip01(value: float) -> float:
    return min(max(value, 0.0), 1.0)


def compute_base_strategy_strength(strategy_family: str, metrics: HistoryMetrics) -> int:
    instability_5 = metrics.sign_flip_count_5 / 4
    s_t = metrics.s_t
    c_t = metrics.c_t

    if strategy_family == "BULL_CALL_SPREAD":
        score_fit = clip01((s_t - 40) / 20)
        conf_fit = clip01((c_t - 0.60) / 0.25)
        persist_fit = clip01((metrics.bull_count_5 - 2) / 3)
        return round(100 * clip01(0.50 * score_fit + 0.30 * conf_fit + 0.25 * persist_fit - 0.15 * instability_5))

    if strategy_family == "BEAR_PUT_SPREAD":
        score_fit = clip01(((-s_t) - 40) / 20)
        conf_fit = clip01((c_t - 0.60) / 0.25)
        persist_fit = clip01((metrics.bear_count_5 - 2) / 3)
        return round(100 * clip01(0.50 * score_fit + 0.30 * conf_fit + 0.25 * persist_fit - 0.15 * instability_5))

    if strategy_family == "BULL_PUT_SPREAD":
        score_fit = clip01(1 - abs(s_t - 25) / 20)
        conf_fit = clip01((c_t - 0.45) / 0.25)
        persist_fit = clip01((metrics.bull_count_5 - 2) / 3)
        return round(100 * clip01(0.45 * score_fit + 0.25 * conf_fit + 0.25 * persist_fit - 0.10 * instability_5))

    if strategy_family == "BEAR_CALL_SPREAD":
        score_fit = clip01(1 - abs(s_t + 25) / 20)
        conf_fit = clip01((c_t - 0.45) / 0.25)
        persist_fit = clip01((metrics.bear_count_5 - 2) / 3)
        return round(100 * clip01(0.45 * score_fit + 0.25 * conf_fit + 0.25 * persist_fit - 0.10 * instability_5))

    if strategy_family == "IRON_CONDOR":
        balance_fit = clip01(1 - abs(s_t) / 10)
        conf_fit = clip01((c_t - 0.50) / 0.25)
        flat_fit = clip01((metrics.flat_count_5 - 3) / 2)
        drift_penalty = clip01(abs(metrics.mean_score_3) / 15)
        return round(100 * clip01(0.45 * balance_fit + 0.25 * conf_fit + 0.25 * flat_fit - 0.20 * drift_penalty))

    return 0


def apply_contract_month_adjustment(base_strength: int, strategy_family: str, contract_month_selection: str) -> int:
    adjustment = 0
    if contract_month_selection == "NEXT_MONTH" and strategy_family in {"BULL_CALL_SPREAD", "BEAR_PUT_SPREAD"}:
        adjustment = -3
    elif contract_month_selection == "NEXT_MONTH" and strategy_family in {"BULL_PUT_SPREAD", "BEAR_CALL_SPREAD"}:
        adjustment = -5
    elif contract_month_selection == "NEXT_MONTH" and strategy_family == "IRON_CONDOR":
        adjustment = -8
    return max(base_strength + adjustment, 0)
