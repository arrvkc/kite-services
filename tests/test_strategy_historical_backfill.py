from __future__ import annotations

from datetime import date, datetime, timezone
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock

import pandas as pd

from engines.strategy_deterministic_engine.adapters.trend_identifier_adapter import (
    TrendIdentifierKiteAdapter,
)
from engines.strategy_deterministic_engine.adapters.trend_identifier_db_adapter import (
    TrendIdentifierDbAdapter,
)
from engines.strategy_deterministic_engine.db.upserts import (
    DELETE_STRATEGY_BATCH_RESULTS_FOR_DATE_SQL,
)
from engines.strategy_deterministic_engine.scripts.run_strategy_engine_batch_from_db import (
    build_argument_parser as strategy_parser,
)
from engines.strategy_deterministic_engine.scripts.sync_contract_snapshot_fo_universe_to_db import (
    build_argument_parser as contract_parser,
)
from engines.strategy_deterministic_engine.scripts.sync_trend_history_fo_universe_to_db import (
    build_argument_parser as trend_parser,
)
from engines.strategy_deterministic_engine.scripts.verify_strategy_backfill_inputs import (
    build_trend_date_patterns,
    validate_strategy_input_coverage,
)
from engines.trend_identifier.trend_identifier.runners.equity_trend_history_runner import (
    EquityTrendHistoryRunner,
)


def trend_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "symbol": "ABC",
                "date": date(2026, 8, day),
                "label": "UP",
                "confidence": 0.8,
                "aggregate_score": 0.5,
                "internal_state": "TRENDING",
                "exchange": "NSE",
                "tradingsymbol": "ABC",
                "instrument_token": 1,
            }
            for day in (14, 17, 18, 19, 20)
        ]
    )


def contract_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "symbol": "ABC",
                "selection_date": date(2026, 8, 20),
                "near_expiry": date(2026, 8, 27),
                "next_expiry": date(2026, 9, 29),
                "dte_near_month": 5,
                "next_month_available": True,
                "dte_next_month": 27,
            }
        ]
    )


class CapturingDbAdapter(TrendIdentifierDbAdapter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sql = []

    def _fetch_dataframe(self, sql, params):
        self.sql.append(sql)
        return trend_frame() if "trend_history_fo_universe" in sql else contract_frame()


class HistoricalBackfillContractTests(unittest.TestCase):
    def test_history_runner_bounds_cut_points_to_explicit_end_date(self):
        runner = EquityTrendHistoryRunner.__new__(EquityTrendHistoryRunner)
        runner.exchange = "NSE"
        equity = MagicMock()
        runner.equity_runner = equity
        hourly = pd.DataFrame(
            {
                "timestamp": pd.to_datetime(
                    [
                        "2026-08-19T10:00:00Z",
                        "2026-08-20T10:00:00Z",
                    ],
                    utc=True,
                )
            }
        )
        daily = pd.DataFrame({"close": [100.0]})
        equity.build_raw_bars_for_symbol_asof.return_value = (
            {"hourly": hourly, "daily": daily},
            {},
        )
        equity.run_for_symbol_asof.return_value = {
            "symbol": "ABC",
            "exchange": "NSE",
            "tradingsymbol": "ABC",
            "instrument_token": 1,
            "payload": {
                "label": "UP",
                "confidence": 0.8,
                "aggregate_score": 0.5,
                "internal_state": "TRENDING",
            },
        }

        result = runner.build_history_for_symbol(
            "ABC", history_days=2, end_date=date(2026, 8, 20)
        )

        self.assertEqual(result.history["date"].tolist(), ["2026-08-19", "2026-08-20"])
        requested_asof = equity.build_raw_bars_for_symbol_asof.call_args_list[0].kwargs[
            "asof_time"
        ]
        self.assertEqual(requested_asof.date(), date(2026, 8, 20))

    def test_history_runner_rejects_non_session_target(self):
        runner = EquityTrendHistoryRunner.__new__(EquityTrendHistoryRunner)
        runner.exchange = "NSE"
        equity = MagicMock()
        runner.equity_runner = equity
        equity.build_raw_bars_for_symbol_asof.return_value = (
            {
                "hourly": pd.DataFrame(
                    {
                        "timestamp": pd.to_datetime(
                            ["2026-08-21T10:00:00Z"], utc=True
                        )
                    }
                ),
                "daily": pd.DataFrame({"close": [100.0]}),
            },
            {},
        )
        with self.assertRaisesRegex(RuntimeError, "No completed market session"):
            runner.build_history_for_symbol(
                "ABC", history_days=1, end_date=date(2026, 8, 22)
            )

    def test_exact_contract_mode_cannot_fall_back(self):
        adapter = CapturingDbAdapter(
            engine=object(),
            run_date=date(2026, 8, 20),
            require_exact_contract_snapshot=True,
        )
        inputs = adapter.build_all()
        self.assertEqual(len(inputs), 1)
        self.assertIn("trade_date <= :run_date", adapter.sql[0])
        self.assertIn("ROW_NUMBER() OVER", adapter.sql[0])
        self.assertIn("PARTITION BY history.symbol", adapter.sql[0])
        self.assertIn("trade_date = :run_date", adapter.sql[0])
        self.assertIn("selection_date = :run_date", adapter.sql[1])
        self.assertNotIn("selection_date <= :run_date", adapter.sql[1])

    def test_ordinary_read_retains_intentional_snapshot_fallback(self):
        adapter = CapturingDbAdapter(
            engine=object(),
            run_date=date(2026, 8, 20),
        )
        adapter.build_all()
        self.assertIn("SELECT DISTINCT trade_date", adapter.sql[0])
        self.assertNotIn("ROW_NUMBER() OVER", adapter.sql[0])
        self.assertIn("selection_date <= :run_date", adapter.sql[1])

    def test_contract_dte_is_relative_to_target_date(self):
        kite = MagicMock()
        kite.instruments.return_value = [
            {
                "instrument_type": "FUT",
                "name": "ABC",
                "expiry": "2026-08-27",
            },
            {
                "instrument_type": "FUT",
                "name": "ABC",
                "expiry": "2026-09-29",
            },
        ]
        adapter = TrendIdentifierKiteAdapter(kite)
        info = adapter.get_contract_info_for_symbol(
            "ABC", datetime(2026, 8, 20, 10, 0, tzinfo=timezone.utc)
        )
        self.assertEqual(info.near_expiry, date(2026, 8, 27))
        self.assertEqual(info.dte_near_month, 5)

    def test_historical_flags_are_explicit_and_fail_closed(self):
        self.assertEqual(
            trend_parser().parse_args(
                ["OMK569", "--end-date", "2026-08-20", "--strict"]
            ).end_date,
            "2026-08-20",
        )
        self.assertEqual(
            contract_parser().parse_args(
                ["OMK569", "--selection-date", "2026-08-20", "--strict"]
            ).selection_date,
            "2026-08-20",
        )
        self.assertTrue(
            strategy_parser().parse_args(
                ["OMK569", "--run-date", "2026-08-20", "--require-exact-contract-snapshot"]
            ).require_exact_contract_snapshot
        )

    def test_strategy_restart_clears_stale_date_rows(self):
        self.assertIn(
            "WHERE run_date = :run_date",
            str(DELETE_STRATEGY_BATCH_RESULTS_FOR_DATE_SQL),
        )

    def test_per_symbol_session_patterns_allow_valid_sparse_market_series(self):
        rows = [
            ("ABC", date(2026, 8, day))
            for day in (14, 17, 18, 19, 20)
        ] + [
            ("SPARSE", date(2026, 8, day))
            for day in (12, 14, 17, 19, 20)
        ]
        patterns = build_trend_date_patterns(rows, date(2026, 8, 20), 5)
        self.assertEqual(sum(item["symbol_count"] for item in patterns), 2)
        self.assertEqual(len(patterns), 2)
        self.assertTrue(all(item["dates"][-1] == "2026-08-20" for item in patterns))

    def test_per_symbol_session_patterns_reject_missing_target_session(self):
        rows = [
            ("ABC", date(2026, 8, day))
            for day in (13, 14, 17, 18, 19)
        ]
        with self.assertRaisesRegex(RuntimeError, "non-target sessions"):
            build_trend_date_patterns(rows, date(2026, 8, 20), 5)

    def test_declared_null_score_exclusions_reconcile_strategy_input_count(self):
        target_symbols = {f"SYM{index}" for index in range(214)}
        excluded_symbols = {f"SYM{index}" for index in range(7)}
        validate_strategy_input_coverage(207, target_symbols, excluded_symbols)

    def test_undeclared_strategy_input_drop_fails_closed(self):
        with self.assertRaisesRegex(RuntimeError, "silently excluded"):
            validate_strategy_input_coverage(1, {"A", "B", "C"}, {"C"})


if __name__ == "__main__":
    unittest.main()
