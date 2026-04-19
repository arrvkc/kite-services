"""Ranking and Top N handling."""
from __future__ import annotations

from collections import defaultdict
from typing import Iterable, List

from .models import HistoryMetrics, StrategyOutputPayload


def family_target_distance(strategy_family: str, s_t: float) -> float:
    if strategy_family == "BULL_CALL_SPREAD":
        return abs(s_t - 50)
    if strategy_family == "BEAR_PUT_SPREAD":
        return abs(s_t + 50)
    if strategy_family == "BULL_PUT_SPREAD":
        return abs(s_t - 25)
    if strategy_family == "BEAR_CALL_SPREAD":
        return abs(s_t + 25)
    if strategy_family == "IRON_CONDOR":
        return abs(s_t)
    return float("inf")


def rank_candidates(payloads: Iterable[StrategyOutputPayload]) -> List[StrategyOutputPayload]:
    """Apply overall and family ranking order."""
    published = [p for p in payloads if p.strategy_family != "NO_TRADE"]

    published.sort(
        key=lambda p: (
            -p.final_strategy_strength,
            -float(p.input_snapshot["confidence"]),
            float(p.input_snapshot["sign_flip_count_5"]) / 4,
            p.instrument,
        )
    )
    for index, payload in enumerate(published, start=1):
        payload.rank_overall = index

    families: dict[str, list[StrategyOutputPayload]] = defaultdict(list)
    for payload in published:
        families[payload.strategy_family].append(payload)

    for family, items in families.items():
        items.sort(
            key=lambda p: (
                -p.final_strategy_strength,
                -float(p.input_snapshot["confidence"]),
                family_target_distance(family, float(p.input_snapshot["aggregate_score"])),
                float(p.input_snapshot["sign_flip_count_5"]) / 4,
                p.instrument,
            )
        )
        for index, payload in enumerate(items, start=1):
            payload.rank_in_family = index

    for payload in payloads:
        payload.include_in_top_n = payload.final_strategy_strength >= 60 and payload.strategy_family != "NO_TRADE"
        if payload.strategy_family == "NO_TRADE":
            payload.rank_overall = None
            payload.rank_in_family = None

    return list(payloads)
