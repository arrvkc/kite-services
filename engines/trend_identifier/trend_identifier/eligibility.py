"""Eligibility gate implementation."""

from __future__ import annotations

from typing import List

import pandas as pd

from .constants import CONSTANTS
from .enums import ReasonCode
from .types import AlignedData, ErrorInfo, GateResult


def _bad_candle(df: pd.DataFrame) -> bool:
    if df.empty:
        return True
    high_bad = df["high"] < df[["open", "close"]].max(axis=1)
    low_bad = df["low"] > df[["open", "close"]].min(axis=1)
    non_positive = (df[["open", "high", "low", "close"]] <= 0).any(axis=1)
    return bool((high_bad | low_bad | non_positive).any())


def _stale(df: pd.DataFrame, asof_time: pd.Timestamp, expected_delta: pd.Timedelta) -> bool:
    if df.empty:
        return True
    latest = df.index.max()
    return bool((asof_time - latest) > expected_delta)


def _missing_ratio(adjusted: pd.DataFrame, lookback: int) -> float:
    tail = adjusted.tail(lookback)
    if len(tail) == 0:
        return 1.0
    return float(tail.isna().any(axis=1).mean())


def _dollar_volume(df: pd.DataFrame) -> pd.Series:
    volume = df.get("volume")
    if volume is None:
        volume = pd.Series(0.0, index=df.index)
    return ((df["close"] * volume).astype(float)).rename("dollar_volume")


def passes_eligibility_gate(aligned: AlignedData) -> GateResult:
    # SPEC TRACE: Section 3 - exact data requirements and eligibility gate
    reasons: List[str] = []
    errors: List[ErrorInfo] = []

    if len(aligned.weekly.bars) < CONSTANTS.weekly_bars_required:
        reasons.append(ReasonCode.GATE_HISTORY_WEEKLY.value)
    if len(aligned.daily.bars) < CONSTANTS.daily_bars_required:
        reasons.append(ReasonCode.GATE_HISTORY_DAILY.value)
    if len(aligned.hourly.bars) < CONSTANTS.hourly_bars_required:
        reasons.append(ReasonCode.GATE_HISTORY_HOURLY.value)

    if _missing_ratio(aligned.daily.adjusted_bars, CONSTANTS.daily_missing_window) > CONSTANTS.daily_missing_tolerance:
        reasons.append(ReasonCode.GATE_MISSING_BARS.value)
    if _missing_ratio(aligned.hourly.adjusted_bars, CONSTANTS.hourly_missing_window) > CONSTANTS.hourly_missing_tolerance:
        if ReasonCode.GATE_MISSING_BARS.value not in reasons:
            reasons.append(ReasonCode.GATE_MISSING_BARS.value)

    if _bad_candle(aligned.weekly.bars) or _bad_candle(aligned.daily.bars) or _bad_candle(aligned.hourly.bars):
        reasons.append(ReasonCode.GATE_BAD_CANDLE.value)

    if _stale(aligned.weekly.bars, aligned.asof_time, pd.Timedelta(days=7)):
        reasons.append(ReasonCode.GATE_STALE_DATA.value)
    if _stale(aligned.daily.bars, aligned.asof_time, pd.Timedelta(days=1)):
        if ReasonCode.GATE_STALE_DATA.value not in reasons:
            reasons.append(ReasonCode.GATE_STALE_DATA.value)
    hourly_delta = pd.Timedelta(hours=int(aligned.metadata.get("hourly_interval_hours", 1)))
    if _stale(aligned.hourly.bars, aligned.asof_time, hourly_delta):
        if ReasonCode.GATE_STALE_DATA.value not in reasons:
            reasons.append(ReasonCode.GATE_STALE_DATA.value)

    if aligned.instrument_type == "equity":
        median_20d = float(_dollar_volume(aligned.daily.bars).tail(20).median()) if len(aligned.daily.bars) >= 20 else 0.0
        if median_20d < CONSTANTS.equity_liquidity_threshold_usd:
            reasons.append(ReasonCode.GATE_LIQUIDITY.value)
    else:
        vol_series = aligned.front_contract_volume_series
        oi_series = aligned.front_contract_oi_series
        threshold = float(aligned.metadata.get("futures_notional_volume_threshold", CONSTANTS.futures_notional_volume_threshold_default))
        if vol_series is None or oi_series is None:
            reasons.append(ReasonCode.GATE_LIQUIDITY.value)
        else:
            median_notional = float(vol_series.tail(20).median()) if len(vol_series) >= 20 else 0.0
            oi_minimum = float(aligned.metadata.get("venue_min_open_interest", 1.0))
            current_oi = float(oi_series.iloc[-1]) if len(oi_series) else 0.0
            if median_notional < threshold or current_oi < oi_minimum:
                reasons.append(ReasonCode.GATE_LIQUIDITY.value)

    passed = len(reasons) == 0
    if not passed:
        for code in reasons:
            errors.append(ErrorInfo(code=code, message=f"Eligibility gate failed: {code}"))
    return GateResult(passed=passed, reason_codes=list(dict.fromkeys(reasons)), errors=errors)
