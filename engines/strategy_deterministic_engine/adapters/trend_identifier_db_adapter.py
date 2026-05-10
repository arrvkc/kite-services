from __future__ import annotations

from datetime import date

import pandas as pd
from sqlalchemy import text

from engines.strategy_deterministic_engine.adapters.trend_identifier_batch_adapter import (
    TrendIdentifierBatchAdapter,
)


class TrendIdentifierDbAdapter:
    def __init__(self, engine, run_date: date, history_days: int = 5):
        self.engine = engine
        self.run_date = run_date
        self.history_days = history_days

    def _fetch_dataframe(self, sql: str, params: dict) -> pd.DataFrame:
        with self.engine.connect() as conn:
            result = conn.execute(text(sql), params)
            rows = result.fetchall()
            columns = result.keys()
        return pd.DataFrame(rows, columns=columns)

    def build_all(self):
        trend_df = self._fetch_dataframe(
            """
            SELECT
                symbol,
                trade_date AS date,
                close,
                label,
                confidence,
                aggregate_score,
                internal_state,
                exchange,
                tradingsymbol,
                instrument_token
            FROM trend_history_fo_universe
            WHERE trade_date IN (
                SELECT DISTINCT trade_date
                FROM trend_history_fo_universe
                WHERE trade_date <= :run_date
                ORDER BY trade_date DESC
                LIMIT :history_days
            )
            ORDER BY symbol, trade_date
            """,
            {
                "run_date": self.run_date,
                "history_days": self.history_days,
            },
        )

        contract_df = self._fetch_dataframe(
            """
            SELECT DISTINCT ON (symbol)
                symbol,
                selection_date,
                near_expiry,
                next_expiry,
                dte_near_month,
                next_month_available,
                dte_next_month
            FROM contract_snapshot_fo_universe
            WHERE selection_date <= :run_date
            ORDER BY symbol, selection_date DESC
            """,
            {"run_date": self.run_date},
        )

        return TrendIdentifierBatchAdapter.from_dataframes(
            trend_history_df=trend_df,
            contract_snapshot_df=contract_df,
        ).build_all()
