from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional

import pandas as pd

from ..models import StrategyInput, TrendPayloadSnapshot, W5HistoryRow


class BatchAdapterError(Exception):
    pass


@dataclass(frozen=True)
class ContractInfo:
    dte_near_month: int
    next_month_available: bool
    dte_next_month: Optional[int]


class TrendIdentifierBatchAdapter:
    def __init__(
        self,
        trend_history_df: pd.DataFrame,
        contract_snapshot_df: pd.DataFrame,
    ) -> None:
        trend_required_columns = {
            "symbol",
            "date",
            "label",
            "confidence",
            "aggregate_score",
            "internal_state",
        }
        contract_required_columns = {
            "symbol",
            "dte_near_month",
            "next_month_available",
            "dte_next_month",
        }

        missing_trend_columns = trend_required_columns - set(trend_history_df.columns)
        missing_contract_columns = contract_required_columns - set(contract_snapshot_df.columns)

        if missing_trend_columns:
            raise BatchAdapterError(f"Missing trend history columns: {missing_trend_columns}")
        if missing_contract_columns:
            raise BatchAdapterError(f"Missing contract snapshot columns: {missing_contract_columns}")

        self.trend_history_df = trend_history_df.copy()
        self.trend_history_df["symbol"] = self.trend_history_df["symbol"].astype(str).str.upper()
        self.trend_history_df["date"] = pd.to_datetime(self.trend_history_df["date"])

        self.contract_snapshot_df = contract_snapshot_df.copy()
        self.contract_snapshot_df["symbol"] = self.contract_snapshot_df["symbol"].astype(str).str.upper()

        self.contract_map: Dict[str, ContractInfo] = {}
        for _, row in self.contract_snapshot_df.iterrows():
            self.contract_map[str(row["symbol"]).upper()] = ContractInfo(
                dte_near_month=int(row["dte_near_month"]),
                next_month_available=bool(row["next_month_available"]),
                dte_next_month=None if pd.isna(row["dte_next_month"]) else int(row["dte_next_month"]),
            )

    @classmethod
    def from_csv(
        cls,
        trend_history_csv_path: str,
        contract_snapshot_csv_path: str,
    ) -> "TrendIdentifierBatchAdapter":
        trend_history_df = pd.read_csv(trend_history_csv_path)
        contract_snapshot_df = pd.read_csv(contract_snapshot_csv_path)
        return cls(trend_history_df=trend_history_df, contract_snapshot_df=contract_snapshot_df)

    def _build_w5(self, symbol: str) -> List[W5HistoryRow]:
        symbol_df = self.trend_history_df[self.trend_history_df["symbol"] == symbol].sort_values("date")
        if len(symbol_df) < 5:
            raise BatchAdapterError(f"{symbol}: insufficient history")
        symbol_df = symbol_df.tail(5)
        rows: List[W5HistoryRow] = []
        for _, row in symbol_df.iterrows():
            if pd.isna(row["aggregate_score"]):
                raise BatchAdapterError(f"{symbol}: null aggregate_score")
            rows.append(
                W5HistoryRow(
                    label=str(row["label"]),
                    confidence=float(row["confidence"]),
                    aggregate_score=float(row["aggregate_score"]),
                )
            )
        return rows

    def _build_latest(self, symbol: str) -> TrendPayloadSnapshot:
        symbol_df = self.trend_history_df[self.trend_history_df["symbol"] == symbol].sort_values("date")
        if symbol_df.empty:
            raise BatchAdapterError(f"{symbol}: no trend history data")
        row = symbol_df.iloc[-1]
        return TrendPayloadSnapshot(
            instrument=symbol,
            asof_time=row["date"].isoformat(),
            label=str(row["label"]),
            confidence=float(row["confidence"]),
            aggregate_score=None if pd.isna(row["aggregate_score"]) else float(row["aggregate_score"]),
            internal_state=str(row["internal_state"]),
        )

    def build_strategy_input(self, symbol: str) -> StrategyInput:
        symbol = symbol.upper()
        if symbol not in self.contract_map:
            raise BatchAdapterError(f"{symbol}: missing contract snapshot")
        latest_payload = self._build_latest(symbol)
        trend_history_w5 = self._build_w5(symbol)
        contract_info = self.contract_map[symbol]
        return StrategyInput(
            instrument=symbol,
            latest_payload=latest_payload,
            trend_history_w5=trend_history_w5,
            dte_near_month=contract_info.dte_near_month,
            next_month_available=contract_info.next_month_available,
            dte_next_month=contract_info.dte_next_month,
            in_universe=True,
            duplicate_payload=False,
            is_completed_daily_run=True,
        )

    def build_all(self) -> List[StrategyInput]:
        symbols = sorted(set(self.trend_history_df["symbol"].unique()).intersection(self.contract_map.keys()))
        results: List[StrategyInput] = []
        for symbol in symbols:
            try:
                results.append(self.build_strategy_input(symbol))
            except Exception:
                continue
        return results
