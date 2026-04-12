from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd
from kiteconnect import KiteConnect

from ..engine import evaluate_trend

REQUIRED_COLUMNS = ["timestamp", "open", "high", "low", "close", "volume"]


@dataclass(frozen=True)
class InstrumentMatch:
    instrument_token: int
    exchange: str
    tradingsymbol: str
    name: str
    segment: str
    instrument_type: str


class EquityTrendRunner:
    """
    Reusable runner that fetches equity data from Kite Connect and executes
    the Trend Identifier engine.
    """

    def __init__(self, kite: KiteConnect, exchange: str = "NSE") -> None:
        self.kite = kite
        self.exchange = exchange.upper()
        self._instrument_cache: Optional[pd.DataFrame] = None

    def _get_instruments(self) -> pd.DataFrame:
        if self._instrument_cache is None:
            instruments = self.kite.instruments(self.exchange)
            df = pd.DataFrame(instruments)
            if df.empty:
                raise RuntimeError(f"No instruments returned for exchange {self.exchange}")
            self._instrument_cache = df
        return self._instrument_cache.copy()

    def resolve_equity_instrument(self, symbol: str) -> InstrumentMatch:
        df = self._get_instruments()
        symbol_upper = symbol.upper()

        exact = df[
            (df["tradingsymbol"].astype(str).str.upper() == symbol_upper)
            & (df["exchange"].astype(str).str.upper() == self.exchange)
        ].copy()

        if exact.empty:
            by_name = df[
                df["name"].fillna("").astype(str).str.upper().str.contains(symbol_upper, regex=False)
                & (df["exchange"].astype(str).str.upper() == self.exchange)
            ].copy()
            if by_name.empty:
                raise RuntimeError(f"Could not resolve symbol {symbol_upper} on {self.exchange}")
            exact = by_name

        if "instrument_type" in exact.columns:
            eq_rows = exact[exact["instrument_type"].fillna("").astype(str).str.upper() == "EQ"].copy()
            if not eq_rows.empty:
                exact = eq_rows

        row = exact.iloc[0]
        return InstrumentMatch(
            instrument_token=int(row["instrument_token"]),
            exchange=str(row["exchange"]),
            tradingsymbol=str(row["tradingsymbol"]),
            name=str(row.get("name", "") or ""),
            segment=str(row.get("segment", "") or ""),
            instrument_type=str(row.get("instrument_type", "") or ""),
        )

    @staticmethod
    def normalize_candles(candles: List[Dict[str, Any]]) -> pd.DataFrame:
        if not candles:
            return pd.DataFrame(columns=REQUIRED_COLUMNS)

        df = pd.DataFrame(candles).copy()
        if "date" in df.columns:
            df = df.rename(columns={"date": "timestamp"})

        missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
        if missing:
            raise ValueError(f"Kite historical data missing required columns: {missing}")

        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = pd.to_numeric(df[col], errors="raise")

        return df[REQUIRED_COLUMNS].sort_values("timestamp").reset_index(drop=True)

    def fetch_historical_data(
        self,
        instrument_token: int,
        from_dt: datetime,
        to_dt: datetime,
        interval: str,
        oi: bool = False,
        continuous: bool = False,
    ) -> pd.DataFrame:
        candles = self.kite.historical_data(
            instrument_token=instrument_token,
            from_date=from_dt,
            to_date=to_dt,
            interval=interval,
            continuous=continuous,
            oi=oi,
        )
        return self.normalize_candles(candles)

    @staticmethod
    def resample_weekly_from_daily(daily_df: pd.DataFrame) -> pd.DataFrame:
        if daily_df.empty:
            return pd.DataFrame(columns=REQUIRED_COLUMNS)

        df = daily_df.copy().set_index("timestamp")
        weekly = (
            df.resample("W-FRI")
            .agg(
                {
                    "open": "first",
                    "high": "max",
                    "low": "min",
                    "close": "last",
                    "volume": "sum",
                }
            )
            .dropna()
            .reset_index()
        )
        return weekly[REQUIRED_COLUMNS]

    @staticmethod
    def _normalize_asof_time(asof_time: Any) -> datetime:
        ts = pd.Timestamp(asof_time)
        if ts.tz is None:
            ts = ts.tz_localize("UTC")
        else:
            ts = ts.tz_convert("UTC")
        return ts.to_pydatetime()

    def build_raw_bars_for_symbol_asof(
        self,
        symbol: str,
        asof_time: Any,
        daily_lookback_days: int = 900,
        hourly_lookback_days: int = 120,
    ) -> tuple[Dict[str, pd.DataFrame], Dict[str, Any]]:
        instrument = self.resolve_equity_instrument(symbol)
        asof_dt = self._normalize_asof_time(asof_time)

        daily_df = self.fetch_historical_data(
            instrument_token=instrument.instrument_token,
            from_dt=asof_dt - timedelta(days=daily_lookback_days),
            to_dt=asof_dt + timedelta(days=1),
            interval="day",
            oi=False,
            continuous=False,
        )
        daily_df = daily_df.loc[daily_df["timestamp"] <= pd.Timestamp(asof_dt)].reset_index(drop=True)

        hourly_df = self.fetch_historical_data(
            instrument_token=instrument.instrument_token,
            from_dt=asof_dt - timedelta(days=hourly_lookback_days),
            to_dt=asof_dt + timedelta(days=1),
            interval="60minute",
            oi=False,
            continuous=False,
        )
        hourly_df = hourly_df.loc[hourly_df["timestamp"] <= pd.Timestamp(asof_dt)].reset_index(drop=True)

        weekly_df = self.resample_weekly_from_daily(daily_df)

        raw_bars = {
            "weekly": weekly_df,
            "daily": daily_df,
            "hourly": hourly_df,
        }

        instrument_metadata = {
            "instrument_type": "equity",
            "hourly_freq": "h",
            "resolved_exchange": instrument.exchange,
            "resolved_tradingsymbol": instrument.tradingsymbol,
            "resolved_name": instrument.name,
            "instrument_token": instrument.instrument_token,
            "segment": instrument.segment,
            "kite_instrument_type": instrument.instrument_type,
        }

        return raw_bars, instrument_metadata

    def build_raw_bars_for_symbol(
        self,
        symbol: str,
        daily_lookback_days: int = 900,
        hourly_lookback_days: int = 120,
    ) -> tuple[Dict[str, pd.DataFrame], Dict[str, Any]]:
        now = datetime.now(timezone.utc)
        return self.build_raw_bars_for_symbol_asof(
            symbol=symbol,
            asof_time=now,
            daily_lookback_days=daily_lookback_days,
            hourly_lookback_days=hourly_lookback_days,
        )

    def run_for_symbol_asof(
        self,
        symbol: str,
        asof_time: Any,
        daily_lookback_days: int = 900,
        hourly_lookback_days: int = 120,
    ) -> Dict[str, Any]:
        raw_bars, instrument_metadata = self.build_raw_bars_for_symbol_asof(
            symbol=symbol,
            asof_time=asof_time,
            daily_lookback_days=daily_lookback_days,
            hourly_lookback_days=hourly_lookback_days,
        )

        for tf_name, df in raw_bars.items():
            if df.empty:
                raise RuntimeError(f"No {tf_name} bars fetched for {symbol}")

        payload = evaluate_trend(
            instrument=symbol.upper(),
            asof_time=asof_time,
            calendar=self.exchange,
            raw_bars=raw_bars,
            instrument_metadata=instrument_metadata,
        )

        return {
            "symbol": symbol.upper(),
            "exchange": instrument_metadata["resolved_exchange"],
            "tradingsymbol": instrument_metadata["resolved_tradingsymbol"],
            "instrument_token": instrument_metadata["instrument_token"],
            "payload": payload,
        }

    def run_for_symbol(
        self,
        symbol: str,
        daily_lookback_days: int = 900,
        hourly_lookback_days: int = 120,
    ) -> Dict[str, Any]:
        raw_bars, instrument_metadata = self.build_raw_bars_for_symbol(
            symbol=symbol,
            daily_lookback_days=daily_lookback_days,
            hourly_lookback_days=hourly_lookback_days,
        )

        for tf_name, df in raw_bars.items():
            if df.empty:
                raise RuntimeError(f"No {tf_name} bars fetched for {symbol}")

        asof_time = raw_bars["hourly"]["timestamp"].iloc[-1]

        payload = evaluate_trend(
            instrument=symbol.upper(),
            asof_time=asof_time,
            calendar=self.exchange,
            raw_bars=raw_bars,
            instrument_metadata=instrument_metadata,
        )

        return {
            "symbol": symbol.upper(),
            "exchange": instrument_metadata["resolved_exchange"],
            "tradingsymbol": instrument_metadata["resolved_tradingsymbol"],
            "instrument_token": instrument_metadata["instrument_token"],
            "payload": payload,
        }

    def run_for_symbols(
        self,
        symbols: Iterable[str],
        daily_lookback_days: int = 900,
        hourly_lookback_days: int = 120,
    ) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []
        for symbol in symbols:
            results.append(
                self.run_for_symbol(
                    symbol=symbol,
                    daily_lookback_days=daily_lookback_days,
                    hourly_lookback_days=hourly_lookback_days,
                )
            )
        return results


def summarize_results(results: List[Dict[str, Any]]) -> pd.DataFrame:
    rows = []
    for item in results:
        payload = item["payload"]
        rows.append(
            {
                "symbol": item["symbol"],
                "exchange": item["exchange"],
                "tradingsymbol": item["tradingsymbol"],
                "instrument_token": item["instrument_token"],
                "label": payload["label"],
                "confidence": payload["confidence"],
                "aggregate_score": payload["aggregate_score"],
                "internal_state": payload["internal_state"],
            }
        )
    return pd.DataFrame(rows)
