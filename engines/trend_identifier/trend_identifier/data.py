"""Data loading and alignment."""

from __future__ import annotations

from typing import Any, Dict, Optional

import pandas as pd

from .types import AlignedData, TimeframeData


def _normalize_pandas_freq(freq: str) -> str:
    # SPEC NOTE: Pandas in this environment rejects uppercase "H" and expects "h".
    if freq == "H":
        return "h"
    return freq


def _ensure_datetime_index(df: pd.DataFrame) -> pd.DataFrame:
    if "timestamp" in df.columns:
        df = df.copy()
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        df = df.set_index("timestamp")
    if not isinstance(df.index, pd.DatetimeIndex):
        raise ValueError("Bars must have a DatetimeIndex or timestamp column.")
    result = df.sort_index().copy()
    if result.index.tz is None:
        result.index = result.index.tz_localize("UTC")
    return result


def _completed_bars_only(df: pd.DataFrame, asof_time: pd.Timestamp) -> pd.DataFrame:
    # SPEC TRACE: Section 12 - completed bars only contract
    return df.loc[df.index <= asof_time].copy()


def _calendar_adjust(df: pd.DataFrame, freq: str) -> tuple[pd.DataFrame, bool]:
    # SPEC TRACE: Section 3 - missing bars may be calendar-adjusted
    if df.empty:
        return df.copy(), False
    normalized_freq = _normalize_pandas_freq(freq)
    full_index = pd.date_range(
        start=df.index.min(),
        end=df.index.max(),
        freq=normalized_freq,
        tz=df.index.tz,
    )
    adjusted = df.reindex(full_index)
    missing_adjusted = adjusted.isna().any(axis=1).any()
    return adjusted, bool(missing_adjusted)


def load_and_align_data(
    instrument_id: str,
    asof_time: Any,
    calendar: str,
    raw_bars: Dict[str, pd.DataFrame],
    instrument_metadata: Optional[Dict[str, Any]] = None,
) -> AlignedData:
    """
    Normalize and align bars.

    raw_bars must provide weekly, daily, hourly DataFrames.
    """
    # SPEC TRACE: Section 12 - load_and_align_data strict function contract
    instrument_metadata = instrument_metadata or {}
    asof_ts = pd.Timestamp(asof_time)
    if asof_ts.tz is None:
        asof_ts = asof_ts.tz_localize("UTC")

    weekly_raw = _completed_bars_only(_ensure_datetime_index(raw_bars["weekly"]), asof_ts)
    daily_raw = _completed_bars_only(_ensure_datetime_index(raw_bars["daily"]), asof_ts)
    hourly_raw = _completed_bars_only(_ensure_datetime_index(raw_bars["hourly"]), asof_ts)

    weekly_adj, weekly_missing = _calendar_adjust(weekly_raw, "W-FRI")
    daily_adj, daily_missing = _calendar_adjust(daily_raw, "B")
    hourly_freq = _normalize_pandas_freq(instrument_metadata.get("hourly_freq", "H"))
    hourly_adj, hourly_missing = _calendar_adjust(hourly_raw, hourly_freq)

    return AlignedData(
        weekly=TimeframeData(bars=weekly_raw, adjusted_bars=weekly_adj, missing_bars_adjusted=weekly_missing),
        daily=TimeframeData(bars=daily_raw, adjusted_bars=daily_adj, missing_bars_adjusted=daily_missing),
        hourly=TimeframeData(bars=hourly_raw, adjusted_bars=hourly_adj, missing_bars_adjusted=hourly_missing),
        instrument=instrument_id,
        instrument_type=instrument_metadata.get("instrument_type", "equity"),
        calendar=calendar,
        asof_time=asof_ts,
        roll_flag=bool(instrument_metadata.get("roll_flag", False)),
        data_quality_warning=bool(instrument_metadata.get("data_quality_warning", False)),
        front_contract_volume_series=instrument_metadata.get("front_contract_volume_series"),
        front_contract_oi_series=instrument_metadata.get("front_contract_oi_series"),
        metadata=instrument_metadata,
    )
