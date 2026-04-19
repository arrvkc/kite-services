"""Derived metric computation."""
from __future__ import annotations

from statistics import mean

from .models import HistoryMetrics, ValidatedInputBundle


def _mapped_sign(score: float) -> int:
    if abs(score) < 10:
        return 0
    if score > 0:
        return 1
    if score < 0:
        return -1
    return 0


def compute_history_metrics(bundle: ValidatedInputBundle) -> HistoryMetrics:
    """Compute the exact W5 derived metrics used by the selector."""
    latest = bundle.latest_payload
    w5 = bundle.trend_history_w5[:5]

    bull_count_5 = sum(1 for row in w5 if row.label == "UP" or (row.label == "FLAT" and row.aggregate_score >= 10))
    bear_count_5 = sum(1 for row in w5 if row.label == "DOWN" or (row.label == "FLAT" and row.aggregate_score <= -10))
    flat_count_5 = sum(1 for row in w5 if row.label == "FLAT" and abs(row.aggregate_score) <= 10)
    mean_abs_score_5 = mean(abs(row.aggregate_score) for row in w5)
    latest_three = w5[:3]
    mean_score_3 = mean(row.aggregate_score for row in latest_three)
    mean_conf_3 = mean(row.confidence for row in latest_three)

    mapped = [_mapped_sign(row.aggregate_score) for row in w5]
    sign_flip_count_5 = sum(1 for left, right in zip(mapped, mapped[1:]) if left != right)

    return HistoryMetrics(
        s_t=float(latest.aggregate_score),
        c_t=float(latest.confidence),
        l_t=latest.label,
        bull_count_5=bull_count_5,
        bear_count_5=bear_count_5,
        flat_count_5=flat_count_5,
        mean_abs_score_5=mean_abs_score_5,
        mean_score_3=mean_score_3,
        mean_conf_3=mean_conf_3,
        sign_flip_count_5=sign_flip_count_5,
    )
