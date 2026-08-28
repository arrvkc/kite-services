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
    persist_strict_trend_preparation,
)
from engines.strategy_deterministic_engine.scripts.run_strategy_engine_batch_from_db import (
    build_argument_parser as strategy_parser,
)
from engines.strategy_deterministic_engine.scripts.sync_contract_snapshot_fo_universe_to_db import (
    build_argument_parser as contract_parser,
)
from engines.strategy_deterministic_engine.scripts.sync_trend_history_fo_universe_to_db import (
    build_strict_preparation_manifest,
    build_argument_parser as trend_parser,
)
from engines.strategy_deterministic_engine.scripts.verify_strategy_backfill_inputs import (
    build_trend_date_patterns,
    validate_no_trading_exclusion,
    validate_preparation_symbol_coverage,
    validate_strategy_input_coverage,
)
from engines.trend_identifier.trend_identifier.runners.equity_trend_history_runner import (
    EquityTrendHistoryRunner,
    NoTradingActivityCandidate,
    NoTradingActivityEvidence,
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
    def test_niftyfpi_nfo_only_resolution_reproduces_missing_target_session(self):
        kite = MagicMock()

        def instruments(exchange):
            if exchange == "NSE":
                return [
                    {
                        "instrument_token": 1,
                        "exchange": "NSE",
                        "tradingsymbol": "ABC",
                        "name": "ABC",
                        "segment": "NSE",
                        "instrument_type": "EQ",
                    }
                ]
            return [
                {
                    "instrument_token": 9200130,
                    "exchange": "NFO",
                    "tradingsymbol": "NIFTYFPI26SEPFUT",
                    "name": "NIFTYFPI",
                    "segment": "NFO-FUT",
                    "instrument_type": "FUT",
                    "expiry": "2026-09-29",
                }
            ]

        kite.instruments.side_effect = instruments
        hourly_dates = (20, 21, 24, 25, 26)
        kite.historical_data.side_effect = lambda **kwargs: [
            {
                "date": f"2026-08-{day:02d}T15:00:00+05:30",
                "open": 1590.0,
                "high": 1600.0,
                "low": 1580.0,
                "close": 1595.8,
                "volume": 100,
                "oi": 50,
            }
            for day in (hourly_dates if kwargs["interval"] == "60minute" else (26,))
        ]
        runner = EquityTrendHistoryRunner(kite=kite, exchange="NSE")

        resolved = runner.equity_runner.resolve_futures_instrument(
            "NIFTYFPI", date(2026, 8, 27)
        )
        self.assertEqual(resolved.exchange, "NFO")
        self.assertEqual(resolved.tradingsymbol, "NIFTYFPI26SEPFUT")

        with self.assertRaisesRegex(
            RuntimeError,
            r"No completed market session found for NIFTYFPI on 2026-08-27\.",
        ):
            runner.build_history_for_symbol(
                "NIFTYFPI", history_days=5, end_date=date(2026, 8, 27)
            )

        self.assertTrue(kite.historical_data.called)
        self.assertTrue(
            all(
                call.kwargs["instrument_token"] == 9200130
                for call in kite.historical_data.call_args_list
            )
        )

    def test_zero_volume_daily_without_hourly_is_audited_no_trading_candidate(self):
        kite = MagicMock()
        kite.instruments.side_effect = lambda exchange: (
            [
                {
                    "instrument_token": 1,
                    "exchange": "NSE",
                    "tradingsymbol": "ABC",
                    "name": "ABC",
                    "segment": "NSE",
                    "instrument_type": "EQ",
                }
            ]
            if exchange == "NSE"
            else [
                {
                    "instrument_token": 9200130,
                    "exchange": "NFO",
                    "tradingsymbol": "NIFTYFPI26SEPFUT",
                    "name": "NIFTYFPI",
                    "segment": "NFO-FUT",
                    "instrument_type": "FUT",
                    "expiry": "2026-09-29",
                }
            ]
        )

        def historical_data(**kwargs):
            if kwargs["interval"] == "day":
                return [
                    {
                        "date": "2026-08-27T00:00:00+05:30",
                        "open": 1595.8,
                        "high": 1595.8,
                        "low": 1595.8,
                        "close": 1595.8,
                        "volume": 0,
                        "oi": 30800,
                    }
                ]
            return [
                {
                    "date": "2026-08-26T15:15:00+05:30",
                    "open": 1590.0,
                    "high": 1600.0,
                    "low": 1580.0,
                    "close": 1595.8,
                    "volume": 100,
                }
            ]

        kite.historical_data.side_effect = historical_data
        runner = EquityTrendHistoryRunner(kite=kite, exchange="NSE")

        with self.assertRaises(NoTradingActivityCandidate) as raised:
            runner.build_history_for_symbol(
                "NIFTYFPI", history_days=5, end_date=date(2026, 8, 27)
            )

        evidence = raised.exception.evidence
        self.assertEqual(raised.exception.reason, "NO_TRADING_ACTIVITY")
        self.assertEqual(evidence.tradingsymbol, "NIFTYFPI26SEPFUT")
        self.assertEqual(evidence.instrument_token, 9200130)
        self.assertEqual(evidence.daily_volume, 0)
        self.assertEqual(evidence.intraday_candle_count, 0)

        manifest = build_strict_preparation_manifest(
            ["ABC", "NIFTYFPI"],
            {"ABC"},
            [raised.exception],
            end_date=date(2026, 8, 27),
        )
        self.assertEqual(manifest["requested_symbols_count"], 2)
        self.assertEqual(manifest["prepared_symbols_count"], 1)
        self.assertEqual(
            manifest["exclusions"][0]["reason"], "NO_TRADING_ACTIVITY"
        )
        self.assertEqual(
            manifest["exclusions"][0]["selected_instrument"]["instrument_token"],
            9200130,
        )

    def test_positive_daily_volume_without_hourly_fails_closed(self):
        runner = EquityTrendHistoryRunner.__new__(EquityTrendHistoryRunner)
        runner.exchange = "NSE"
        equity = MagicMock()
        runner.equity_runner = equity
        equity.build_raw_bars_for_symbol_asof.return_value = (
            {
                "hourly": pd.DataFrame(
                    {
                        "timestamp": pd.to_datetime(
                            ["2026-08-26T09:45:00Z"], utc=True
                        )
                    }
                ),
                "daily": pd.DataFrame(
                    {
                        "timestamp": pd.to_datetime(
                            ["2026-08-27T00:00:00+05:30"], utc=True
                        ),
                        "volume": [1],
                    }
                ),
            },
            {
                "resolved_exchange": "NFO",
                "resolved_tradingsymbol": "NEW26SEPFUT",
                "instrument_token": 99,
            },
        )
        with self.assertRaisesRegex(RuntimeError, "No completed market session"):
            runner.build_history_for_symbol(
                "NEW", history_days=1, end_date=date(2026, 8, 27)
            )

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

    def test_declared_no_trading_exclusion_reconciles_prepared_universe(self):
        evidence = NoTradingActivityEvidence(
            symbol="NIFTYFPI",
            target_date=date(2026, 8, 27),
            exchange="NFO",
            tradingsymbol="NIFTYFPI26SEPFUT",
            instrument_token=9200130,
            required_interval="60minute",
            intraday_candle_count=0,
            daily_timestamp="2026-08-26T18:30:00+00:00",
            daily_open=1595.8,
            daily_high=1595.8,
            daily_low=1595.8,
            daily_close=1595.8,
            daily_volume=0,
            daily_oi=30800,
        )
        exclusion = build_strict_preparation_manifest(
            ["ABC", "NIFTYFPI"],
            {"ABC"},
            [NoTradingActivityCandidate(evidence)],
            end_date=date(2026, 8, 27),
        )["exclusions"][0]
        validate_no_trading_exclusion(exclusion, date(2026, 8, 27))
        validate_preparation_symbol_coverage(
            {"ABC"},
            {"ABC", "NIFTYFPI"},
            {"NIFTYFPI"},
            requested_symbols_count=2,
            prepared_symbols_count=1,
        )

    def test_strict_preparation_persists_rows_and_manifest_atomically(self):
        connection = MagicMock()
        connection.execute.side_effect = [MagicMock(), MagicMock(fetchone=lambda: (7,))]
        transaction = MagicMock()
        transaction.__enter__.return_value = connection
        engine = MagicMock()
        engine.begin.return_value = transaction
        exclusion = {
            "symbol": "NIFTYFPI",
            "reason": "NO_TRADING_ACTIVITY",
            "stage": "TREND_PREPARATION",
        }

        written = persist_strict_trend_preparation(
            engine,
            [
                {
                    "user_id": "OMK569",
                    "symbol": "ABC",
                    "trade_date": date(2026, 8, 27),
                    "close": 100,
                    "label": "UP",
                    "confidence": 0.8,
                    "aggregate_score": 0.5,
                    "internal_state": "TRENDING",
                    "exchange": "NSE",
                    "tradingsymbol": "ABC",
                    "instrument_token": 1,
                }
            ],
            run_date=date(2026, 8, 27),
            generated_by_user_id="OMK569",
            requested_symbols_count=2,
            prepared_symbols_count=1,
            exclusions=[exclusion],
        )

        self.assertEqual(written, 1)
        self.assertEqual(connection.execute.call_count, 2)
        manifest_params = connection.execute.call_args_list[1].args[1]
        self.assertEqual(manifest_params["requested_symbols_count"], 2)
        self.assertEqual(manifest_params["prepared_symbols_count"], 1)
        self.assertIn("NO_TRADING_ACTIVITY", manifest_params["input_exclusions_json"])

    def test_undeclared_missing_prepared_symbol_is_rejected(self):
        with self.assertRaisesRegex(RuntimeError, "symbol sets differ"):
            validate_preparation_symbol_coverage(
                {"ABC"},
                {"ABC", "MISSING"},
                set(),
                requested_symbols_count=2,
                prepared_symbols_count=1,
            )


if __name__ == "__main__":
    unittest.main()
