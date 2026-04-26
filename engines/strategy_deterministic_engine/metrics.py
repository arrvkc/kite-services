from __future__ import annotations

from typing import Any

from .constants import LABEL_DOWN, LABEL_FLAT, LABEL_UP
from .models import HistoryMetrics


def _get_row_value(row: Any, key: str) -> Any:
    if isinstance(row, dict):
        return row[key]
    return getattr(row, key)


def _mapped_sign(score: float) -> int:
    if abs(score) < 10:
        return 0
    if score > 0:
        return 1
    return -1


def compute_history_metrics(trend_history_w5: list[Any]) -> HistoryMetrics:
    window = trend_history_w5[-5:]
    bull_count_5 = 0
    bear_count_5 = 0
    flat_count_5 = 0
    abs_scores = []
    last_three_scores = []
    last_three_conf = []

    for row in window:
        label = _get_row_value(row, "label")
        score = float(_get_row_value(row, "aggregate_score"))
        confidence = float(_get_row_value(row, "confidence"))

        abs_scores.append(abs(score))

        if label == LABEL_UP or (label == LABEL_FLAT and score >= 10):
            bull_count_5 += 1
        if label == LABEL_DOWN or (label == LABEL_FLAT and score <= -10):
            bear_count_5 += 1
        if label == LABEL_FLAT and abs(score) <= 10:
            flat_count_5 += 1

        last_three_scores.append(score)
        last_three_conf.append(confidence)

    last_three_scores = last_three_scores[-3:]
    last_three_conf = last_three_conf[-3:]
    signs = [_mapped_sign(float(_get_row_value(row, "aggregate_score"))) for row in window]

    sign_flip_count_5 = 0
    for left, right in zip(signs, signs[1:]):
        if left != right:
            sign_flip_count_5 += 1

    instability_5 = sign_flip_count_5 / 4

    return HistoryMetrics(
        bull_count_5=bull_count_5,
        bear_count_5=bear_count_5,
        flat_count_5=flat_count_5,
        mean_abs_score_5=sum(abs_scores) / 5,
        mean_score_3=sum(last_three_scores) / 3,
        mean_conf_3=sum(last_three_conf) / 3,
        sign_flip_count_5=sign_flip_count_5,
        instability_5=instability_5,
    )
