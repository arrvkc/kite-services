from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
from typing import Any, Dict, List
from zoneinfo import ZoneInfo

import pandas as pd
from kiteconnect import KiteConnect

from .equity_trend_runner import EquityTrendRunner


NO_TRADING_ACTIVITY = "NO_TRADING_ACTIVITY"
TARGET_DATE_HISTORY_UNAVAILABLE = "TARGET_DATE_HISTORY_UNAVAILABLE"
# This is deliberately symbol-scoped. Missing target bars for any other symbol
# remain an unexpected fatal error until separately governed with evidence.
GOVERNED_TARGET_DATE_UNAVAILABLE_SYMBOLS = frozenset({"NIFTYFPI"})


@dataclass(frozen=True)
class TrendHistoryResult:
    symbol: str
    exchange: str
    tradingsymbol: str
    instrument_token: int
    history: pd.DataFrame
    source_window: "CoherentHistoricalWindow"


@dataclass(frozen=True)
class CoherentHistoricalWindow:
    """Exact Zerodha bars used for every Trend cut point in one symbol run."""

    symbol: str
    exchange: str
    tradingsymbol: str
    instrument_token: int
    target_session_date: date
    retrieved_at: datetime
    finality_status: str
    daily: pd.DataFrame
    hourly: pd.DataFrame
    daily_content_hash: str
    hourly_content_hash: str


class TargetSessionNotFinalized(RuntimeError):
    """The target daily candle is not yet safe for immutable publication."""


def _canonical_decimal(value: object) -> str | None:
    if value is None or pd.isna(value):
        return None
    try:
        decimal_value = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError("Historical evidence contains an invalid number.") from exc
    if not decimal_value.is_finite():
        raise ValueError("Historical evidence contains a non-finite number.")
    if decimal_value == 0:
        return "0"
    return format(decimal_value.normalize(), "f")


def canonical_bars(frame: pd.DataFrame) -> list[dict]:
    rows = []
    for row in frame.to_dict(orient="records"):
        timestamp = pd.Timestamp(row["timestamp"])
        if timestamp.tzinfo is None:
            timestamp = timestamp.tz_localize("UTC")
        else:
            timestamp = timestamp.tz_convert("UTC")
        rows.append(
            {
                "timestamp": timestamp.isoformat(),
                "open": _canonical_decimal(row.get("open")),
                "high": _canonical_decimal(row.get("high")),
                "low": _canonical_decimal(row.get("low")),
                "close": _canonical_decimal(row.get("close")),
                "volume": _canonical_decimal(row.get("volume")),
                "oi": _canonical_decimal(row.get("oi")),
            }
        )
    return rows


def bars_content_hash(frame: pd.DataFrame) -> str:
    payload = json.dumps(
        canonical_bars(frame),
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


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


@dataclass(frozen=True)
class TargetDateHistoryUnavailableEvidence:
    symbol: str
    target_date: date
    exchange: str
    tradingsymbol: str
    instrument_token: int
    required_interval: str
    target_daily_candle_count: int
    target_intraday_candle_count: int
    latest_daily_timestamp: str
    latest_intraday_timestamp: str


class TargetDateHistoryUnavailableCandidate(RuntimeError):
    """Allowlisted symbol lacks both target-date daily and intraday evidence."""

    reason = TARGET_DATE_HISTORY_UNAVAILABLE

    def __init__(self, evidence: TargetDateHistoryUnavailableEvidence) -> None:
        self.evidence = evidence
        super().__init__(
            f"{evidence.symbol} has no daily or {evidence.required_interval} "
            f"observations on {evidence.target_date}; latest source observations "
            "predate the target session."
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
        *,
        retrieved_at: datetime | None = None,
    ) -> TrendHistoryResult:
        if history_days <= 0:
            raise ValueError("history_days must be positive.")

        # Acquire the complete daily/hourly source once.  Every historical cut
        # point below is a slice of this exact version, so a run cannot stitch
        # together different Zerodha historical revision bases.
        target_date = (
            date.fromisoformat(end_date) if isinstance(end_date, str) else end_date
        )
        if target_date is None:
            raw_bars, instrument_metadata = self.equity_runner.build_raw_bars_for_symbol(
                symbol=symbol,
                daily_lookback_days=daily_lookback_days,
                hourly_lookback_days=hourly_lookback_days,
            )
            target_date = (
                pd.to_datetime(raw_bars["hourly"]["timestamp"], utc=True)
                .dt.tz_convert("Asia/Kolkata")
                .dt.date.max()
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
                daily_lookback_days=daily_lookback_days,
                hourly_lookback_days=hourly_lookback_days,
            )

        retrieved_at = retrieved_at or datetime.now(timezone.utc)
        if retrieved_at.tzinfo is None:
            raise ValueError("retrieved_at must be timezone-aware.")

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
            normalized_symbol = symbol.strip().upper()
            if (
                target_hourly.empty
                and target_daily.empty
                and normalized_symbol in GOVERNED_TARGET_DATE_UNAVAILABLE_SYMBOLS
                and not hourly.empty
                and not daily.empty
            ):
                raise TargetDateHistoryUnavailableCandidate(
                    TargetDateHistoryUnavailableEvidence(
                        symbol=normalized_symbol,
                        target_date=target_date,
                        exchange=str(instrument_metadata["resolved_exchange"]),
                        tradingsymbol=str(
                            instrument_metadata["resolved_tradingsymbol"]
                        ),
                        instrument_token=int(instrument_metadata["instrument_token"]),
                        required_interval="60minute",
                        target_daily_candle_count=0,
                        target_intraday_candle_count=0,
                        latest_daily_timestamp=pd.Timestamp(
                            daily["timestamp"].max()
                        ).isoformat(),
                        latest_intraday_timestamp=pd.Timestamp(
                            hourly["timestamp"].max()
                        ).isoformat(),
                    )
                )

            if len(target_daily.index) != 1:
                raise TargetSessionNotFinalized(
                    f"No completed market session found for {symbol.upper()} "
                    f"on {target_date}: expected one target-session daily candle."
                )

            retrieved_local = retrieved_at.astimezone(ZoneInfo("Asia/Kolkata"))
            if retrieved_local.date() == target_date:
                latest_target_hour = None
                if not target_hourly.empty:
                    latest_target_hour = (
                        target_hourly["timestamp"]
                        .dt.tz_convert("Asia/Kolkata")
                        .max()
                    )
                if (
                    retrieved_local.time() < time(16, 0)
                    or latest_target_hour is None
                    or latest_target_hour.time() < time(15, 15)
                ):
                    raise TargetSessionNotFinalized(
                        f"{symbol.upper()} target session {target_date} is not finalized."
                    )
            elif retrieved_local.date() < target_date:
                raise TargetSessionNotFinalized(
                    f"{symbol.upper()} target session {target_date} is in the future."
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
            raw_bars_asof = {
                "daily": raw_bars["daily"].loc[
                    pd.to_datetime(raw_bars["daily"]["timestamp"], utc=True)
                    <= pd.Timestamp(asof_time)
                ].reset_index(drop=True),
                "hourly": raw_bars["hourly"].loc[
                    pd.to_datetime(raw_bars["hourly"]["timestamp"], utc=True)
                    <= pd.Timestamp(asof_time)
                ].reset_index(drop=True),
            }
            daily_close = raw_bars_asof["daily"].iloc[-1]["close"]

            result = self.equity_runner.evaluate_raw_bars_asof(
                symbol=symbol,
                asof_time=asof_time,
                raw_bars=raw_bars,
                instrument_metadata=instrument_metadata,
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
            source_window=CoherentHistoricalWindow(
                symbol=symbol.upper(),
                exchange=last_result["exchange"],
                tradingsymbol=last_result["tradingsymbol"],
                instrument_token=last_result["instrument_token"],
                target_session_date=target_date,
                retrieved_at=retrieved_at,
                finality_status="FINALIZED",
                daily=raw_bars["daily"].copy(),
                hourly=raw_bars["hourly"].copy(),
                daily_content_hash=bars_content_hash(raw_bars["daily"]),
                hourly_content_hash=bars_content_hash(raw_bars["hourly"]),
            ),
        )
