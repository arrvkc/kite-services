"""Feature computation."""

from __future__ import annotations

from typing import Dict, List, Tuple

import numpy as np
import pandas as pd

from .constants import CONSTANTS
from .enums import ReasonCode
from .exceptions import MissingIntermediateError, NumericInvalidError
from .types import AlignedData, FeatureBundle, TimeframeFeatures


def _assert_finite(value: float) -> None:
    if value is None:
        return
    if not np.isfinite(value):
        raise NumericInvalidError("Non-finite numeric encountered.")


def _ema(series: pd.Series, span: int) -> pd.Series:
    # SPEC TRACE: Section 10 - EMA formula
    return series.ewm(span=span, adjust=False).mean()


def _true_range(df: pd.DataFrame) -> pd.Series:
    # SPEC TRACE: Section 10 - true range formula
    prev_close = df["close"].shift(1)
    return pd.concat(
        [
            df["high"] - df["low"],
            (df["high"] - prev_close).abs(),
            (df["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)


def _atr14(df: pd.DataFrame) -> pd.Series:
    # SPEC TRACE: Section 10 - ATR14 Wilder smoothing
    tr = _true_range(df)
    atr = tr.ewm(alpha=1 / 14, adjust=False).mean()
    return atr


def _returns(df: pd.DataFrame) -> pd.Series:
    close = df["close"].astype(float)
    return np.log(close / close.shift(1))


def _realized_vol(returns: pd.Series, window: int, annual_factor: float) -> float:
    # SPEC TRACE: Section 5.1 - exact realized volatility definition
    vals = returns.dropna()
    if len(vals) < window:
        raise MissingIntermediateError(ReasonCode.REALIZED_VOL_INSUFFICIENT.value)
    result = float(np.sqrt(annual_factor) * vals.tail(window).std(ddof=0))
    _assert_finite(result)
    return result


def _hhv(df: pd.DataFrame, lookback: int) -> float:
    return float(df["high"].iloc[-lookback-1:-1].max())


def _llv(df: pd.DataFrame, lookback: int) -> float:
    return float(df["low"].iloc[-lookback-1:-1].min())


def _pivot_state(df: pd.DataFrame, left: int, right: int) -> Tuple[int, bool]:
    # SPEC TRACE: Section 5.3 - exact tie rules for pivots
    highs = df["high"].reset_index(drop=True)
    lows = df["low"].reset_index(drop=True)
    pivot_highs: List[int] = []
    pivot_lows: List[int] = []
    tie_flag = False
    for j in range(left, len(df) - right):
        hw = highs.iloc[j-left:j+right+1]
        lw = lows.iloc[j-left:j+right+1]
        if highs.iloc[j] == hw.max() and (hw == hw.max()).sum() > 1:
            tie_flag = True
        if lows.iloc[j] == lw.min() and (lw == lw.min()).sum() > 1:
            tie_flag = True
        if highs.iloc[j] > hw.drop(j).max():
            pivot_highs.append(j)
        if lows.iloc[j] < lw.drop(j).min():
            pivot_lows.append(j)
    sw = 0
    if len(pivot_highs) >= 2 and len(pivot_lows) >= 2:
        hh1, hh2 = highs.iloc[pivot_highs[-2]], highs.iloc[pivot_highs[-1]]
        ll1, ll2 = lows.iloc[pivot_lows[-2]], lows.iloc[pivot_lows[-1]]
        if hh2 > hh1 and ll2 > ll1:
            sw = 1
        elif hh2 < hh1 and ll2 < ll1:
            sw = -1
    return sw, tie_flag


def _breakout_acceptance(df: pd.DataFrame, lookback: int, acc_n: int, atr14: float) -> Tuple[int, float, bool]:
    # SPEC TRACE: Section 5.3 - breakout acceptance and partial handling
    if len(df) < lookback + 1:
        raise MissingIntermediateError(ReasonCode.MISSING_INTERMEDIATE_LOAD_BEARING.value)
    close = df["close"].reset_index(drop=True)
    high = df["high"].reset_index(drop=True)
    low = df["low"].reset_index(drop=True)
    hhv = high.iloc[-lookback-1:-1].max()
    llv = low.iloc[-lookback-1:-1].min()

    if close.iloc[-1] >= hhv + 0.25 * atr14:
        future = close.iloc[-min(acc_n, len(close)): ]
        acc = float((future >= hhv).mean())
        partial = len(future) < acc_n
        sign = 1 if (acc >= 0.60 and not partial) else 0
        # SPEC NOTE: partial acceptance cannot upgrade from FLAT to directional; sign held at 0 when partial
        return sign, acc, partial
    if close.iloc[-1] <= llv - 0.25 * atr14:
        future = close.iloc[-min(acc_n, len(close)): ]
        acc = float((future <= llv).mean())
        partial = len(future) < acc_n
        sign = -1 if (acc >= 0.60 and not partial) else 0
        return sign, acc, partial
    ema_slow = _ema(df["close"], min(lookback, max(5, lookback))).iloc[-1]
    acc = float((close.tail(acc_n) >= ema_slow).mean())
    return 0, acc, False


def _compute_timeframe_features(df: pd.DataFrame, timeframe: str, instrument_type: str, annual_factor: float) -> TimeframeFeatures:
    lookbacks = getattr(CONSTANTS, f"{timeframe}_lookbacks")
    if len(df) < max(lookbacks.values()) + 2:
        raise MissingIntermediateError(ReasonCode.MISSING_INTERMEDIATE_LOAD_BEARING.value)

    close = df["close"].astype(float)
    open_ = df["open"].astype(float)
    atr = _atr14(df).iloc[-1]
    if pd.isna(atr) or atr <= 0:
        raise MissingIntermediateError(ReasonCode.MISSING_INTERMEDIATE_LOAD_BEARING.value)

    returns = _returns(df)
    rv_short = _realized_vol(returns, lookbacks["rv_short"], annual_factor)
    rv_long = _realized_vol(returns, lookbacks["rv_long"], annual_factor)

    ema_fast = _ema(close, lookbacks["fast"])
    ema_slow = _ema(close, lookbacks["slow"])
    nr = float((close.iloc[-1] - close.iloc[-1 - lookbacks["ret"]]) / atr)
    bs = float((ema_fast.iloc[-1] - ema_fast.iloc[-1 - lookbacks["slope"]]) / (lookbacks["slope"] * atr))
    sep = float((ema_fast.iloc[-1] - ema_slow.iloc[-1]) / atr)
    hhv = _hhv(df, lookbacks["break"])
    llv = _llv(df, lookbacks["break"])
    bod_up = float((close.iloc[-1] - hhv) / atr)
    bod_dn = float((llv - close.iloc[-1]) / atr)
    sw, tie_flag = _pivot_state(df, lookbacks["pivot_left"], lookbacks["pivot_right"])
    breakout_sign, breakout_acc, acceptance_partial = _breakout_acceptance(df, lookbacks["break"], lookbacks["acc"], atr)

    eff_num = abs(close.iloc[-1] - close.iloc[-1 - lookbacks["eff"]])
    eff_den = float(np.abs(close.diff().tail(lookbacks["eff"])).sum())
    if eff_den == 0:
        raise NumericInvalidError("EFF denominator is zero.")
    eff = float(eff_num / eff_den)

    if breakout_sign == 0:
        acc = float((close.tail(lookbacks["acc"]) >= ema_slow.iloc[-1]).mean())
    else:
        acc = breakout_acc

    ovl_window = close.tail(lookbacks["ovl"])
    range_window = df["high"].tail(lookbacks["ovl"]).max() - df["low"].tail(lookbacks["ovl"]).min()
    ovl = 1.0 - abs(float(ovl_window.median()) - float(close.iloc[-1])) / max(float(range_window), 0.5 * float(atr))
    ovl = float(min(max(ovl, 0.0), 1.0))

    gap = float(abs(open_.iloc[-1] - close.iloc[-2]) / atr)
    volx = float(rv_short / rv_long)
    tr = _true_range(df)
    anom = int(tr.iloc[-1] > CONSTANTS.anomaly_true_range_multiplier * float(tr.shift(1).tail(20).median()))
    volume = df.get("volume", pd.Series(np.zeros(len(df)), index=df.index))
    liq_threshold = CONSTANTS.equity_liquidity_threshold_usd if instrument_type == "equity" else CONSTANTS.futures_notional_volume_threshold_default
    liq = int(float((close * volume).tail(20).median()) < liq_threshold)

    for value in [nr, bs, sep, bod_up, bod_dn, eff, acc, ovl, gap, volx, rv_short, rv_long]:
        _assert_finite(value)

    return TimeframeFeatures(
        nr=nr,
        bs=bs,
        sep=sep,
        bod_up=bod_up,
        bod_dn=bod_dn,
        sw=sw,
        breakout_sign=breakout_sign,
        eff=eff,
        acc=acc,
        ovl=ovl,
        gap=gap,
        volx=volx,
        anom=anom,
        liq=liq,
        atr14=float(atr),
        acceptance_partial=acceptance_partial,
        pivot_tie=tie_flag,
        realized_vol_short=rv_short,
        realized_vol_long=rv_long,
    )


def compute_features(aligned: AlignedData) -> FeatureBundle:
    # SPEC TRACE: Section 12 - compute_features strict function contract
    reasons: List[str] = []
    diagnostics: Dict[str, bool] = {}
    try:
        weekly = _compute_timeframe_features(aligned.weekly.bars.dropna(), "weekly", aligned.instrument_type, 252.0)
        daily = _compute_timeframe_features(aligned.daily.bars.dropna(), "daily", aligned.instrument_type, 252.0)
        session_bars = float(aligned.metadata.get("session_bars_per_day", CONSTANTS.session_bars_per_day_cash))
        hourly = _compute_timeframe_features(aligned.hourly.bars.dropna(), "hourly", aligned.instrument_type, 252.0 * session_bars)
    except MissingIntermediateError as exc:
        code = str(exc)
        if code not in [r.value for r in ReasonCode]:
            code = ReasonCode.MISSING_INTERMEDIATE_LOAD_BEARING.value
        reasons.append(code)
        raise
    except NumericInvalidError:
        reasons.append(ReasonCode.NUMERIC_INVALID.value)
        raise

    if weekly.acceptance_partial or daily.acceptance_partial or hourly.acceptance_partial:
        reasons.append(ReasonCode.ACCEPTANCE_PARTIAL.value)
    diagnostics["acceptance_partial"] = weekly.acceptance_partial or daily.acceptance_partial or hourly.acceptance_partial
    diagnostics["pivot_tie"] = weekly.pivot_tie or daily.pivot_tie or hourly.pivot_tie
    return FeatureBundle(weekly=weekly, daily=daily, hourly=hourly, reason_codes=list(dict.fromkeys(reasons)), diagnostics=diagnostics)
