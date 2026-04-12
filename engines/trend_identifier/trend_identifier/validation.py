"""Validation metrics from the specification."""

from __future__ import annotations

from collections import defaultdict
from typing import Dict, Iterable

import numpy as np
import pandas as pd


def turnover(labels: pd.Series) -> float:
    # SPEC TRACE: Section 17 - turnover metric
    if len(labels) < 2:
        return 0.0
    changes = (labels != labels.shift(1)).sum() - 1
    return float(max(changes, 0) / len(labels) * 252.0)


def direct_reversal_fraction(labels: pd.Series) -> float:
    # SPEC TRACE: Section 17 - direct reversal metric
    if len(labels) < 3:
        return 0.0
    reversals = 0
    transitions = 0
    prev = labels.iloc[0]
    for curr in labels.iloc[1:]:
        if curr != prev:
            transitions += 1
            if {prev, curr} == {"UP", "DOWN"}:
                reversals += 1
            prev = curr
    return float(reversals / transitions) if transitions else 0.0


def label_persistence(labels: pd.Series) -> Dict[str, float]:
    # SPEC TRACE: Section 17 - label persistence metric
    runs = defaultdict(list)
    if labels.empty:
        return {}
    curr = labels.iloc[0]
    count = 1
    for label in labels.iloc[1:]:
        if label == curr:
            count += 1
        else:
            runs[curr].append(count)
            curr = label
            count = 1
    runs[curr].append(count)
    return {k: float(np.median(v)) for k, v in runs.items()}


def false_directional_break_rate(labels: pd.Series, forward_returns_atr: pd.Series, side: str) -> float:
    # SPEC TRACE: Section 17 - false directional break metric
    if side == "UP":
        mask = labels == "UP"
        bad = (forward_returns_atr <= -1.0) & mask
    else:
        mask = labels == "DOWN"
        bad = (forward_returns_atr >= 1.0) & mask
    denom = int(mask.sum())
    return float(bad.sum() / denom) if denom else 0.0


def flat_miss_rate(labels: pd.Series, abs_move_atr: pd.Series, efficiency: pd.Series) -> float:
    # SPEC TRACE: Section 17 - FLAT miss metric
    mask = labels == "FLAT"
    miss = mask & (abs_move_atr >= 2.0) & (efficiency >= 0.45)
    denom = int(mask.sum())
    return float(miss.sum() / denom) if denom else 0.0


def chop_capture_share(labels: pd.Series, range_bound_mask: pd.Series) -> float:
    # SPEC TRACE: Section 17 - chop capture metric
    denom = int(range_bound_mask.sum())
    if denom == 0:
        return 0.0
    return float(((labels == "FLAT") & range_bound_mask).sum() / denom)


def shock_robustness(labels: pd.Series, false_break_mask: pd.Series, shock_mask: pd.Series) -> Dict[str, float]:
    # SPEC TRACE: Section 17 - shock robustness metric
    normal = ~shock_mask
    shock_rate = float((false_break_mask & shock_mask).sum() / max(int(shock_mask.sum()), 1))
    normal_rate = float((false_break_mask & normal).sum() / max(int(normal.sum()), 1))
    return {"shock_false_break_rate": shock_rate, "normal_false_break_rate": normal_rate, "delta": shock_rate - normal_rate}


def sector_diversity_report(frame: pd.DataFrame, sector_col: str = "sector") -> Dict[str, Dict[str, float]]:
    # SPEC TRACE: Section 17 - sector diversity reporting hooks
    report: Dict[str, Dict[str, float]] = {}
    for sector, group in frame.groupby(sector_col):
        report[str(sector)] = {"turnover": turnover(group["label"])}
    return report


def futures_robustness_report(frame: pd.DataFrame, family_col: str = "family") -> Dict[str, Dict[str, float]]:
    # SPEC TRACE: Section 17 - futures robustness reporting hooks
    report: Dict[str, Dict[str, float]] = {}
    for family, group in frame.groupby(family_col):
        report[str(family)] = {"turnover": turnover(group["label"])}
    return report
