from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Iterable, List, Optional, Sequence, Set

import pandas as pd
from kiteconnect import KiteConnect

from ..models import StrategyInput, TrendPayloadSnapshot, W5HistoryRow
from engines.trend_identifier.trend_identifier.runners.equity_trend_history_runner import (
    EquityTrendHistoryRunner,
)
from engines.trend_identifier.trend_identifier.runners.equity_trend_runner import (
    EquityTrendRunner,
)


class TrendIdentifierAdapterError(Exception):
    pass


@dataclass(frozen=True)
class FOSymbolContractInfo:
    symbol: str
    dte_near_month: int
    next_month_available: bool
    dte_next_month: Optional[int]
    near_expiry: date
    next_expiry: Optional[date]


class TrendIdentifierKiteAdapter:
    def __init__(
        self,
        kite: KiteConnect,
        exchange: str = "NSE",
        derivatives_exchange: str = "NFO",
        holiday_dates: Optional[Sequence[date | str | pd.Timestamp]] = None,
    ) -> None:
        self.kite = kite
        self.exchange = exchange.upper()
        self.derivatives_exchange = derivatives_exchange.upper()
        self.equity_runner = EquityTrendRunner(kite=kite, exchange=self.exchange)
        self.history_runner = EquityTrendHistoryRunner(kite=kite, exchange=self.exchange)
        self._nse_instruments_df: Optional[pd.DataFrame] = None
        self._nfo_instruments_df: Optional[pd.DataFrame] = None
        self._fo_stock_symbols: Optional[List[str]] = None
        self._holiday_dates: Set[date] = {
            pd.Timestamp(item).date() for item in (holiday_dates or [])
        }

    def _get_nse_instruments_df(self) -> pd.DataFrame:
        if self._nse_instruments_df is None:
            rows = self.kite.instruments(self.exchange)
            df = pd.DataFrame(rows)
            if df.empty:
                raise TrendIdentifierAdapterError(
                    f"No instruments returned for exchange {self.exchange}"
                )
            self._nse_instruments_df = df
        return self._nse_instruments_df.copy()

    def _get_nfo_instruments_df(self) -> pd.DataFrame:
        if self._nfo_instruments_df is None:
            rows = self.kite.instruments(self.derivatives_exchange)
            df = pd.DataFrame(rows)
            if df.empty:
                raise TrendIdentifierAdapterError(
                    f"No instruments returned for exchange {self.derivatives_exchange}"
                )
            self._nfo_instruments_df = df
        return self._nfo_instruments_df.copy()

    def _get_nse_equity_symbols(self) -> Set[str]:
        df = self._get_nse_instruments_df()
        if "instrument_type" in df.columns:
            eq = df[df["instrument_type"].fillna("").astype(str).str.upper() == "EQ"].copy()
        else:
            eq = df.copy()
        if "tradingsymbol" not in eq.columns:
            raise TrendIdentifierAdapterError("NSE instruments missing tradingsymbol")
        return set(eq["tradingsymbol"].astype(str).str.upper().tolist())

    def get_fo_stock_symbols(self) -> List[str]:
        if self._fo_stock_symbols is not None:
            return list(self._fo_stock_symbols)

        nse_equity_symbols = self._get_nse_equity_symbols()
        nfo = self._get_nfo_instruments_df()

        required_cols = {"instrument_type", "name", "expiry"}
        missing = [c for c in required_cols if c not in nfo.columns]
        if missing:
            raise TrendIdentifierAdapterError(
                f"NFO instruments missing required columns: {missing}"
            )

        fut = nfo[nfo["instrument_type"].fillna("").astype(str).str.upper() == "FUT"].copy()
        fut = fut[fut["name"].fillna("").astype(str).str.upper().isin(nse_equity_symbols)].copy()

        if fut.empty:
            raise TrendIdentifierAdapterError("No stock futures symbols found in NFO instruments")

        symbols = sorted(fut["name"].astype(str).str.upper().unique().tolist())
        self._fo_stock_symbols = symbols
        return list(symbols)

    def _normalize_asof_time(self, asof_time: object) -> pd.Timestamp:
        ts = pd.Timestamp(asof_time)
        if ts.tzinfo is None:
            return ts.tz_localize("Asia/Kolkata")
        return ts.tz_convert("Asia/Kolkata")

    def _selection_date_from_asof(self, asof_time: object) -> date:
        return self._normalize_asof_time(asof_time).date()

    def _monthly_futures_for_symbol(self, symbol: str) -> pd.DataFrame:
        nfo = self._get_nfo_instruments_df()

        required_cols = {"instrument_type", "name", "expiry"}
        missing = [c for c in required_cols if c not in nfo.columns]
        if missing:
            raise TrendIdentifierAdapterError(
                f"NFO instruments missing required columns: {missing}"
            )

        fut = nfo[nfo["instrument_type"].fillna("").astype(str).str.upper() == "FUT"].copy()
        fut = fut[fut["name"].fillna("").astype(str).str.upper() == symbol.upper()].copy()

        if fut.empty:
            raise TrendIdentifierAdapterError(f"No NFO futures found for symbol {symbol.upper()}")

        fut["expiry"] = pd.to_datetime(fut["expiry"]).dt.date
        fut = fut.sort_values(["expiry"]).reset_index(drop=True)
        return fut

    def _count_trading_sessions(
        self,
        selection_day: date,
        expiry_day: date,
    ) -> int:
        if expiry_day < selection_day:
            return -1
        start = selection_day + timedelta(days=1)
        if expiry_day < start:
            return 0
        sessions = pd.bdate_range(start=start, end=expiry_day)
        if not self._holiday_dates:
            return int(len(sessions))
        count = 0
        for ts in sessions:
            if ts.date() not in self._holiday_dates:
                count += 1
        return count

    def get_contract_info_for_symbol(
        self,
        symbol: str,
        asof_time: object,
    ) -> FOSymbolContractInfo:
        symbol = symbol.upper()
        fut = self._monthly_futures_for_symbol(symbol)
        selection_day = self._selection_date_from_asof(asof_time)

        unique_expiries = sorted({d for d in fut["expiry"].tolist() if d >= selection_day})
        if not unique_expiries:
            raise TrendIdentifierAdapterError(
                f"No current or future monthly expiries found for {symbol}"
            )

        near_expiry = unique_expiries[0]
        next_expiry = unique_expiries[1] if len(unique_expiries) >= 2 else None

        dte_near_month = self._count_trading_sessions(selection_day, near_expiry)
        next_month_available = next_expiry is not None
        dte_next_month = (
            self._count_trading_sessions(selection_day, next_expiry)
            if next_expiry is not None
            else None
        )

        return FOSymbolContractInfo(
            symbol=symbol,
            dte_near_month=dte_near_month,
            next_month_available=next_month_available,
            dte_next_month=dte_next_month,
            near_expiry=near_expiry,
            next_expiry=next_expiry,
        )

    def _build_latest_payload(self, symbol: str) -> TrendPayloadSnapshot:
        latest_result = self.equity_runner.run_for_symbol(symbol=symbol)
        payload = latest_result["payload"]

        required = [
            "label",
            "confidence",
            "aggregate_score",
            "internal_state",
            "asof_time",
        ]
        missing = [k for k in required if k not in payload]
        if missing:
            raise TrendIdentifierAdapterError(
                f"Latest Trend Identifier payload missing fields: {missing}"
            )

        return TrendPayloadSnapshot(
            instrument=symbol.upper(),
            asof_time=str(payload["asof_time"]),
            label=str(payload["label"]),
            confidence=float(payload["confidence"]),
            aggregate_score=(
                None
                if payload["aggregate_score"] is None
                else float(payload["aggregate_score"])
            ),
            internal_state=str(payload["internal_state"]),
        )

    def _build_w5_history(self, symbol: str, history_days: int = 5) -> List[W5HistoryRow]:
        result = self.history_runner.build_history_for_symbol(
            symbol=symbol,
            history_days=history_days,
        )
        history_df = result.history.copy()
        if len(history_df) < 5:
            raise TrendIdentifierAdapterError(
                f"Insufficient Trend Identifier history for {symbol.upper()}: {len(history_df)} rows"
            )

        w5_df = history_df.tail(5).reset_index(drop=True)
        rows: List[W5HistoryRow] = []
        for row in w5_df.to_dict(orient="records"):
            aggregate_score = row["aggregate_score"]
            if aggregate_score is None:
                raise TrendIdentifierAdapterError(
                    f"Null aggregate_score found in W5 history for {symbol.upper()}"
                )
            rows.append(
                W5HistoryRow(
                    label=str(row["label"]),
                    confidence=float(row["confidence"]),
                    aggregate_score=float(aggregate_score),
                )
            )
        return rows

    def build_strategy_input_for_symbol(
        self,
        symbol: str,
        duplicate_payload: bool = False,
        is_completed_daily_run: bool = True,
    ) -> StrategyInput:
        symbol = symbol.upper()
        fo_symbols = set(self.get_fo_stock_symbols())
        in_universe = symbol in fo_symbols

        latest_payload = self._build_latest_payload(symbol)
        trend_history_w5 = self._build_w5_history(symbol, history_days=5)
        contract_info = self.get_contract_info_for_symbol(symbol, latest_payload.asof_time)

        return StrategyInput(
            instrument=symbol,
            latest_payload=latest_payload,
            trend_history_w5=trend_history_w5,
            dte_near_month=contract_info.dte_near_month,
            next_month_available=contract_info.next_month_available,
            dte_next_month=contract_info.dte_next_month,
            in_universe=in_universe,
            duplicate_payload=bool(duplicate_payload),
            is_completed_daily_run=bool(is_completed_daily_run),
        )

    def build_strategy_inputs_for_symbols(
        self,
        symbols: Iterable[str],
        duplicate_payload: bool = False,
        is_completed_daily_run: bool = True,
    ) -> List[StrategyInput]:
        results: List[StrategyInput] = []
        for symbol in symbols:
            results.append(
                self.build_strategy_input_for_symbol(
                    symbol=symbol,
                    duplicate_payload=duplicate_payload,
                    is_completed_daily_run=is_completed_daily_run,
                )
            )
        return results

    def build_strategy_inputs_for_fo_universe(
        self,
        duplicate_payload: bool = False,
        is_completed_daily_run: bool = True,
    ) -> List[StrategyInput]:
        symbols = self.get_fo_stock_symbols()
        return self.build_strategy_inputs_for_symbols(
            symbols=symbols,
            duplicate_payload=duplicate_payload,
            is_completed_daily_run=is_completed_daily_run,
        )