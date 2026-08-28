from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time
from typing import Any, Dict, List
from zoneinfo import ZoneInfo

import pandas as pd
from kiteconnect import KiteConnect

from .equity_trend_runner import EquityTrendRunner


NO_TRADING_ACTIVITY = "NO_TRADING_ACTIVITY"


@dataclass(frozen=True)
class TrendHistoryResult:
    symbol: str
    exchange: str
    tradingsymbol: str
    instrument_token: int
    history: pd.DataFrame


@dataclass(frozen=True)
class NoTradingActivityEvidence:
    symbol: str
    target_date: date
    exchange: str
    tradingsymbol: str
    instrument_token: int
    required_interval: str
    intraday_candle_count: int
    daily_timestamp: str
    daily_open: object
    daily_high: object
    daily_low: object
    daily_close: object
    daily_volume: object
    daily_oi: object | None


class NoTradingActivityCandidate(RuntimeError):
    """Exact-date zero-volume evidence awaiting market-session confirmation."""

    reason = NO_TRADING_ACTIVITY

    def __init__(self, evidence: NoTradingActivityEvidence) -> None:
        self.evidence = evidence
        super().__init__(
            f"{evidence.symbol} has a zero-volume daily candle and no "
            f"{evidence.required_interval} bars on {evidence.target_date}."
        )


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
        end_date: date | str | None = None,
    ) -> TrendHistoryResult:
        if history_days <= 0:
            raise ValueError("history_days must be positive.")

        # Keep this only for discovering the last N trading-day cut points.
        # Historical recovery must bound this discovery before selecting the
        # final N sessions; otherwise today's latest candles contaminate an
        # earlier run date.
        target_date = date.fromisoformat(end_date) if isinstance(end_date, str) else end_date
        if target_date is None:
            raw_bars, _ = self.equity_runner.build_raw_bars_for_symbol(
                symbol=symbol,
                daily_lookback_days=30,
                hourly_lookback_days=hourly_lookback_days,
            )
        else:
            target_end = datetime.combine(
                target_date,
                time.max,
                tzinfo=ZoneInfo("Asia/Kolkata"),
            )
            raw_bars, instrument_metadata = self.equity_runner.build_raw_bars_for_symbol_asof(
                symbol=symbol,
                asof_time=target_end,
                daily_lookback_days=30,
                hourly_lookback_days=hourly_lookback_days,
            )

        hourly = raw_bars["hourly"].copy()
        hourly["timestamp"] = pd.to_datetime(hourly["timestamp"], utc=True)

        if target_date is not None:
            target_hourly = hourly.loc[
                hourly["timestamp"]
                .dt.tz_convert("Asia/Kolkata")
                .dt.date
                .eq(target_date)
            ]
            daily = raw_bars["daily"].copy()
            if not daily.empty and {"timestamp", "volume"}.issubset(daily.columns):
                daily["timestamp"] = pd.to_datetime(daily["timestamp"], utc=True)
                target_daily = daily.loc[
                    daily["timestamp"]
                    .dt.tz_convert("Asia/Kolkata")
                    .dt.date
                    .eq(target_date)
                ]
            else:
                target_daily = daily
            if target_hourly.empty and len(target_daily.index) == 1:
                daily_row = target_daily.iloc[0]
                if daily_row.get("volume") == 0:
                    raise NoTradingActivityCandidate(
                        NoTradingActivityEvidence(
                            symbol=symbol.upper(),
                            target_date=target_date,
                            exchange=str(instrument_metadata["resolved_exchange"]),
                            tradingsymbol=str(
                                instrument_metadata["resolved_tradingsymbol"]
                            ),
                            instrument_token=int(
                                instrument_metadata["instrument_token"]
                            ),
                            required_interval="60minute",
                            intraday_candle_count=0,
                            daily_timestamp=pd.Timestamp(
                                daily_row["timestamp"]
                            ).isoformat(),
                            daily_open=daily_row.get("open"),
                            daily_high=daily_row.get("high"),
                            daily_low=daily_row.get("low"),
                            daily_close=daily_row.get("close"),
                            daily_volume=daily_row.get("volume"),
                            daily_oi=daily_row.get("oi"),
                        )
                    )

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
        if target_date is not None:
            last_session_date = (
                pd.Timestamp(selected_cut_points[-1])
                .tz_convert("Asia/Kolkata")
                .date()
            )
            if last_session_date != target_date:
                raise RuntimeError(
                    f"No completed market session found for {symbol.upper()} on {target_date}."
                )

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
