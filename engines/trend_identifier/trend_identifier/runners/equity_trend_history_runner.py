from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List

import pandas as pd
from kiteconnect import KiteConnect

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

        # Keep this only for discovering the last N trading-day cut points.
        raw_bars, _ = self.equity_runner.build_raw_bars_for_symbol(
            symbol=symbol,
            daily_lookback_days=30,
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
        last_result: Dict[str, Any] | None = None

        for asof_time in selected_cut_points:
            raw_bars_asof, _ = self.equity_runner.build_raw_bars_for_symbol_asof(
                symbol=symbol,
                asof_time=asof_time,
                daily_lookback_days=daily_lookback_days,
                hourly_lookback_days=hourly_lookback_days,
            )
            daily_close = raw_bars_asof["daily"].iloc[-1]["close"]

            result = self.equity_runner.run_for_symbol_asof(
                symbol=symbol,
                asof_time=asof_time,
                daily_lookback_days=daily_lookback_days,
                hourly_lookback_days=hourly_lookback_days,
            )
            last_result = result
            payload = result["payload"]

            rows.append(
                {
                    "date": pd.Timestamp(asof_time).date().isoformat(),
                    "symbol": result["symbol"],
                    "exchange": result["exchange"],
                    "tradingsymbol": result["tradingsymbol"],
                    "instrument_token": result["instrument_token"],
                    "close": daily_close,
                    "label": payload["label"],
                    "confidence": payload["confidence"],
                    "aggregate_score": payload["aggregate_score"],
                    "internal_state": payload["internal_state"],
                }
            )

        history_df = pd.DataFrame(rows).sort_values("date").reset_index(drop=True)

        preferred_columns = [
            "date",
            "close",
            "label",
            "confidence",
            "aggregate_score",
            "internal_state",
        ]
        history_df = history_df[preferred_columns]

        if last_result is None:
            raise RuntimeError("No history rows were produced.")

        return TrendHistoryResult(
            symbol=symbol.upper(),
            exchange=last_result["exchange"],
            tradingsymbol=last_result["tradingsymbol"],
            instrument_token=last_result["instrument_token"],
            history=history_df,
        )
