from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List

import pandas as pd
from kiteconnect import KiteConnect

from ..engine import evaluate_trend
from .equity_trend_runner import EquityTrendRunner


@dataclass(frozen=True)
class TrendHistoryResult:
    symbol: str
    exchange: str
    tradingsymbol: str
    instrument_token: int
    history: pd.DataFrame


class EquityTrendHistoryRunner:
    """
    Reusable runner that builds a daily history of Trend Identifier outputs
    for an equity using Kite Connect historical data.

    History is based on the last N trading-day evaluation cut points derived
    from hourly candles.
    """

    def __init__(self, kite: KiteConnect, exchange: str = "NSE") -> None:
        self.kite = kite
        self.exchange = exchange.upper()
        self.equity_runner = EquityTrendRunner(kite=kite, exchange=self.exchange)

    def build_history_for_symbol(
        self,
        symbol: str,
        history_days: int,
        daily_lookback_days: int = 900,
        hourly_lookback_days: int = 120,
    ) -> TrendHistoryResult:
        if history_days <= 0:
            raise ValueError("history_days must be positive.")

        raw_bars, instrument_metadata = self.equity_runner.build_raw_bars_for_symbol(
            symbol=symbol,
            daily_lookback_days=daily_lookback_days,
            hourly_lookback_days=hourly_lookback_days,
        )

        hourly = raw_bars["hourly"].copy()
        hourly["timestamp"] = pd.to_datetime(hourly["timestamp"], utc=True)

        if hourly.empty:
            raise RuntimeError(f"No hourly bars found for {symbol}.")

        daily_cut_points = (
            hourly.groupby(hourly["timestamp"].dt.normalize())["timestamp"]
            .max()
            .sort_values()
            .tolist()
        )

        if not daily_cut_points:
            raise RuntimeError(f"No daily evaluation cut points available for {symbol}.")

        selected_cut_points = daily_cut_points[-history_days:]

        rows: List[Dict[str, Any]] = []

        for asof_time in selected_cut_points:
            subset_raw_bars = {
                "weekly": raw_bars["weekly"].loc[raw_bars["weekly"]["timestamp"] <= asof_time].copy(),
                "daily": raw_bars["daily"].loc[raw_bars["daily"]["timestamp"] <= asof_time].copy(),
                "hourly": raw_bars["hourly"].loc[raw_bars["hourly"]["timestamp"] <= asof_time].copy(),
            }

            payload = evaluate_trend(
                instrument=symbol.upper(),
                asof_time=asof_time,
                calendar=self.exchange,
                raw_bars=subset_raw_bars,
                instrument_metadata=instrument_metadata,
            )

            rows.append(
                {
                    "date": pd.Timestamp(asof_time).date().isoformat(),
                    "symbol": symbol.upper(),
                    "exchange": instrument_metadata["resolved_exchange"],
                    "tradingsymbol": instrument_metadata["resolved_tradingsymbol"],
                    "instrument_token": instrument_metadata["instrument_token"],
                    "label": payload["label"],
                    "confidence": payload["confidence"],
                    "aggregate_score": payload["aggregate_score"],
                    "internal_state": payload["internal_state"],
                }
            )

        history_df = pd.DataFrame(rows).sort_values("date").reset_index(drop=True)

        preferred_columns = [
            "date",
            "symbol",
            "exchange",
            "tradingsymbol",
            "instrument_token",
            "label",
            "confidence",
            "aggregate_score",
            "internal_state",
        ]
        history_df = history_df[preferred_columns]

        return TrendHistoryResult(
            symbol=symbol.upper(),
            exchange=instrument_metadata["resolved_exchange"],
            tradingsymbol=instrument_metadata["resolved_tradingsymbol"],
            instrument_token=instrument_metadata["instrument_token"],
            history=history_df,
        )
