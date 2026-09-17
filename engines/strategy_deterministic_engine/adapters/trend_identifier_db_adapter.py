from __future__ import annotations

from datetime import date

import pandas as pd
from sqlalchemy import text

from engines.strategy_deterministic_engine.adapters.trend_identifier_batch_adapter import (
    TrendIdentifierBatchAdapter,
)


class TrendIdentifierDbAdapter:
    def __init__(
        self,
        engine,
        run_date: date,
        user_id: str,
        history_days: int = 5,
        require_exact_contract_snapshot: bool = False,
    ):
        self.engine = engine
        self.run_date = run_date
        self.user_id = str(user_id).strip().upper()
        if not self.user_id:
            raise ValueError("An authoritative Strategy user id is required.")
        self.history_days = history_days
        self.require_exact_contract_snapshot = require_exact_contract_snapshot

    def _fetch_dataframe(self, sql: str, params: dict) -> pd.DataFrame:
        with self.engine.connect() as conn:
            result = conn.execute(text(sql), params)
            rows = result.fetchall()
            columns = result.keys()
        return pd.DataFrame(rows, columns=columns)

    def load_source_frames(self) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Load Trend and contract evidence for one authoritative account."""
        if self.require_exact_contract_snapshot:
            trend_sql = """
                WITH target_symbols AS (
                    SELECT DISTINCT symbol
                    FROM trend_history_fo_universe
                    WHERE user_id = :user_id
                      AND trade_date = :run_date
                ), ranked_history AS (
                    SELECT
                        history.symbol,
                        history.trade_date,
                        history.close,
                        history.label,
                        history.confidence,
                        history.aggregate_score,
                        history.internal_state,
                        history.exchange,
                        history.tradingsymbol,
                        history.instrument_token,
                        ROW_NUMBER() OVER (
                            PARTITION BY history.symbol
                            ORDER BY history.trade_date DESC
                        ) AS session_rank
                    FROM trend_history_fo_universe AS history
                    JOIN target_symbols USING (symbol)
                    WHERE history.user_id = :user_id
                      AND history.trade_date <= :run_date
                )
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
                FROM ranked_history
                WHERE session_rank <= :history_days
                ORDER BY symbol, trade_date
            """
        else:
            trend_sql = """
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
                WHERE user_id = :user_id
                  AND trade_date IN (
                    SELECT DISTINCT trade_date
                    FROM trend_history_fo_universe
                    WHERE user_id = :user_id
                      AND trade_date <= :run_date
                    ORDER BY trade_date DESC
                    LIMIT :history_days
                )
                ORDER BY symbol, trade_date
            """
        trend_df = self._fetch_dataframe(
            trend_sql,
            {
                "run_date": self.run_date,
                "history_days": self.history_days,
                "user_id": self.user_id,
            },
        )

        comparison = "=" if self.require_exact_contract_snapshot else "<="
        distinct = "" if self.require_exact_contract_snapshot else "DISTINCT ON (symbol)"
        contract_df = self._fetch_dataframe(
            f"""
            SELECT {distinct}
                symbol,
                selection_date,
                near_expiry,
                next_expiry,
                dte_near_month,
                next_month_available,
                dte_next_month
            FROM contract_snapshot_fo_universe
            WHERE user_id = :user_id
              AND selection_date {comparison} :run_date
            ORDER BY symbol, selection_date DESC
            """,
            {"run_date": self.run_date, "user_id": self.user_id},
        )

        return trend_df, contract_df

    def build_all(self):
        trend_df, contract_df = self.load_source_frames()

        return TrendIdentifierBatchAdapter.from_dataframes(
            trend_history_df=trend_df,
            contract_snapshot_df=contract_df,
        ).build_all()
