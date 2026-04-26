from __future__ import annotations

from collections import defaultdict

from .constants import MIN_TOPN_STRENGTH, REASON_STRENGTH_ABOVE_TOPN_MIN, REASON_STRENGTH_BELOW_TOPN_MIN
from .strength import family_target_distance

def rank_batch(payloads: list[dict]) -> list[dict]:
    for payload in payloads:
        include_in_top_n = payload["final_strategy_strength"] >= MIN_TOPN_STRENGTH
        payload["include_in_top_n"] = include_in_top_n
        strength_reason = REASON_STRENGTH_ABOVE_TOPN_MIN if include_in_top_n else REASON_STRENGTH_BELOW_TOPN_MIN
        if strength_reason not in payload["reason_codes"]:
            payload["reason_codes"].append(strength_reason)
    overall_eligible = [p for p in payloads if p["include_in_top_n"]]
    overall_sorted = sorted(
        overall_eligible,
        key=lambda payload: (
            -payload["final_strategy_strength"],
            -payload["input_snapshot"]["confidence"],
            payload["input_snapshot"]["sign_flip_count_5"] / 4,
            payload["instrument"],
        ),
    )
    for index, payload in enumerate(overall_sorted, start=1):
        payload["rank_overall"] = index
    family_buckets: dict[str, list[dict]] = defaultdict(list)
    for payload in overall_eligible:
        family_buckets[payload["strategy_family"]].append(payload)
    for family_payloads in family_buckets.values():
        family_payloads.sort(
            key=lambda payload: (
                -payload["final_strategy_strength"],
                -payload["input_snapshot"]["confidence"],
                family_target_distance(payload["strategy_family"], payload["input_snapshot"]["aggregate_score"]),
                payload["input_snapshot"]["sign_flip_count_5"] / 4,
                payload["instrument"],
            ),
        )
        for index, payload in enumerate(family_payloads, start=1):
            payload["rank_in_family"] = index
    for payload in payloads:
        if not payload["include_in_top_n"]:
            payload["rank_overall"] = None
            payload["rank_in_family"] = None
    return payloads
