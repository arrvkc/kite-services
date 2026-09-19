from __future__ import annotations

from datetime import date, datetime, timezone
from contextlib import redirect_stdout
import io
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock
from unittest.mock import patch
import sys

import pandas as pd
from kiteconnect.exceptions import TokenException

from engines.strategy_deterministic_engine.adapters.trend_identifier_adapter import (
    TrendIdentifierKiteAdapter,
)
from engines.strategy_deterministic_engine.adapters.trend_identifier_db_adapter import (
    TrendIdentifierDbAdapter,
)
from engines.strategy_deterministic_engine.db.upserts import (
    DELETE_STRATEGY_BATCH_RESULTS_FOR_DATE_SQL,
    PREPARE_STRATEGY_RUN_SQL,
    persist_strict_trend_preparation,
)
from engines.strategy_deterministic_engine.scripts import (
    sync_contract_snapshot_fo_universe_to_db as contract_sync,
)
from engines.strategy_deterministic_engine.scripts import (
    sync_trend_history_fo_universe_to_db as trend_sync,
)
from engines.strategy_deterministic_engine.scripts.run_strategy_engine_batch_from_db import (
    build_argument_parser as strategy_parser,
)
from engines.strategy_deterministic_engine.scripts.sync_contract_snapshot_fo_universe_to_db import (
    build_argument_parser as contract_parser,
)
from engines.strategy_deterministic_engine.scripts.sync_trend_history_fo_universe_to_db import (
    acquisition_content_hash,
    build_acquisition_evidence,
    build_strict_preparation_manifest,
    build_argument_parser as trend_parser,
    exception_detail,
)
from engines.strategy_deterministic_engine.scripts.verify_strategy_backfill_inputs import (
    build_trend_date_patterns,
    validate_no_trading_exclusion,
    validate_preparation_symbol_coverage,
    validate_strategy_input_coverage,
    validate_target_date_unavailable_exclusion,
)
from engines.trend_identifier.trend_identifier.runners.equity_trend_history_runner import (
    CoherentHistoricalWindow,
    EquityTrendHistoryRunner,
    NoTradingActivityCandidate,
    NoTradingActivityEvidence,
    TargetDateHistoryUnavailableCandidate,
    TargetDateHistoryUnavailableEvidence,
    TargetSessionNotFinalized,
    bars_content_hash,
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


def coherent_source_window(
    symbol: str = "ABC", target_date: date = date(2026, 9, 8)
) -> CoherentHistoricalWindow:
    daily = pd.DataFrame(
        [{
            "timestamp": pd.Timestamp(target_date, tz="Asia/Kolkata"),
            "open": 99.0,
            "high": 101.0,
            "low": 98.0,
            "close": 100.0,
            "volume": 1000,
        }]
    )
    hourly = pd.DataFrame(
        [{
            "timestamp": pd.Timestamp(
                f"{target_date.isoformat()}T15:15:00+05:30"
            ),
            "open": 99.0,
            "high": 101.0,
            "low": 98.0,
            "close": 100.0,
            "volume": 1000,
        }]
    )
    return CoherentHistoricalWindow(
        symbol=symbol,
        exchange="NSE",
        tradingsymbol=symbol,
        instrument_token=1,
        target_session_date=target_date,
        retrieved_at=datetime(2026, 9, 9, tzinfo=timezone.utc),
        finality_status="FINALIZED",
        daily=daily,
        hourly=hourly,
        daily_content_hash=bars_content_hash(daily),
        hourly_content_hash=bars_content_hash(hourly),
    )


def strict_result(symbol: str = "ABC") -> SimpleNamespace:
    return SimpleNamespace(
        symbol=symbol,
        exchange="NSE",
        tradingsymbol=symbol,
        instrument_token=1,
        history=pd.DataFrame(
            [{
                "date": "2026-09-08",
                "close": 100,
                "label": "UP",
                "confidence": 0.8,
                "aggregate_score": 0.5,
                "internal_state": "CLASSIFIABLE",
            }]
        ),
        source_window=coherent_source_window(symbol),
    )


def acquisition_fixture(acquisition_id: str = "acquisition-1") -> dict:
    source_window = coherent_source_window()
    return {
        "id": acquisition_id,
        "source_account": "OMK569",
        "target_session_date": source_window.target_session_date,
        "retrieved_at": source_window.retrieved_at,
        "status": "COMPLETE",
        "finality_status": "FINALIZED",
        "content_hash": "a" * 64,
        "snapshots": [],
        "bars": [],
        "trend_outputs": [],
    }


class CapturingDbAdapter(TrendIdentifierDbAdapter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sql = []

    def _fetch_dataframe(self, sql, params):
        self.sql.append(sql)
        return (
            trend_frame()
            if "trend_history_fo_universe" in sql
            or "historical_trend_outputs" in sql
            else contract_frame()
        )

    def _load_acquisition_id(self):
        return "acquisition-1" if self.require_exact_contract_snapshot else None


class HistoricalBackfillContractTests(unittest.TestCase):
    def _coherent_history_result(self, symbol, closes, *, retrieved_at=None):
        runner = EquityTrendHistoryRunner.__new__(EquityTrendHistoryRunner)
        runner.exchange = "NSE"
        equity = MagicMock()
        runner.equity_runner = equity
        dates = pd.date_range("2026-05-18", periods=len(closes), freq="B")
        daily = pd.DataFrame(
            {
                "timestamp": pd.to_datetime(dates).tz_localize("Asia/Kolkata"),
                "open": closes,
                "high": [value + 2 for value in closes],
                "low": [value - 2 for value in closes],
                "close": closes,
                "volume": [1000] * len(closes),
            }
        )
        hourly = pd.DataFrame(
            {
                "timestamp": [
                    pd.Timestamp(day.date(), tz="Asia/Kolkata")
                    + pd.Timedelta(hours=15, minutes=15)
                    for day in dates
                ],
                "open": closes,
                "high": [value + 2 for value in closes],
                "low": [value - 2 for value in closes],
                "close": closes,
                "volume": [1000] * len(closes),
            }
        )
        equity.build_raw_bars_for_symbol_asof.return_value = (
            {"daily": daily, "hourly": hourly},
            {
                "resolved_exchange": "NSE",
                "resolved_tradingsymbol": symbol,
                "instrument_token": 123,
            },
        )
        equity.evaluate_raw_bars_asof.return_value = {
            "symbol": symbol,
            "exchange": "NSE",
            "tradingsymbol": symbol,
            "instrument_token": 123,
            "payload": {
                "label": "UP",
                "confidence": 0.8,
                "aggregate_score": 0.5,
                "internal_state": "TRENDING",
            },
        }
        result = runner.build_history_for_symbol(
            symbol,
            history_days=len(closes),
            end_date=dates[-1].date(),
            retrieved_at=retrieved_at
            or datetime(2026, 6, 30, tzinfo=timezone.utc),
        )
        return result, equity

    def test_adjusted_bonus_examples_remain_one_unmodified_coherent_window(self):
        examples = {
            "LICI": [401.20, 398.90, 402.10, 399.80, 398.10],
            "TRENT": [2798.40, 2812.10, 2801.50, 2820.20, 2816.00],
        }
        for symbol, closes in examples.items():
            with self.subTest(symbol=symbol):
                result, equity = self._coherent_history_result(symbol, closes)
                self.assertEqual(result.history["close"].tolist(), closes)
                self.assertLess(
                    max(abs(b / a - 1) for a, b in zip(closes, closes[1:])),
                    0.05,
                )
                self.assertEqual(
                    equity.build_raw_bars_for_symbol_asof.call_count,
                    1,
                )
                self.assertEqual(
                    equity.evaluate_raw_bars_asof.call_count,
                    len(closes),
                )

    def test_later_source_revision_has_new_content_hash_without_mutating_first(self):
        first, _ = self._coherent_history_result(
            "NORMAL", [100.0, 101.0, 102.0]
        )
        revised, _ = self._coherent_history_result(
            "NORMAL", [100.0, 101.0, 102.25]
        )
        first_hash = first.source_window.daily_content_hash
        self.assertNotEqual(first_hash, revised.source_window.daily_content_hash)
        self.assertEqual(first.source_window.daily.iloc[-1]["close"], 102.0)
        first_evidence = build_acquisition_evidence(
            "acquisition-1", SimpleNamespace(user_id="XJ1877"), first
        )
        revised_evidence = build_acquisition_evidence(
            "acquisition-2", SimpleNamespace(user_id="XJ1877"), revised
        )
        self.assertNotEqual(
            acquisition_content_hash(first_evidence["snapshots"]),
            acquisition_content_hash(revised_evidence["snapshots"]),
        )
        self.assertTrue(all(
            item["acquisition_id"] == "acquisition-1"
            for item in first_evidence["snapshots"]
        ))
        self.assertTrue(all(
            item["acquisition_id"] == "acquisition-2"
            for item in revised_evidence["snapshots"]
        ))

    def test_same_day_target_requires_completed_session_evidence(self):
        target = date(2026, 5, 22)
        with self.assertRaisesRegex(TargetSessionNotFinalized, "not finalized"):
            self._coherent_history_result(
                "ABC",
                [100.0, 101.0, 102.0, 103.0, 104.0],
                retrieved_at=datetime(
                    2026, 5, 22, 9, 59, tzinfo=timezone.utc
                ),
            )
        result, _ = self._coherent_history_result(
            "ABC",
            [100.0, 101.0, 102.0, 103.0, 104.0],
            retrieved_at=datetime(2026, 5, 22, 10, 31, tzinfo=timezone.utc),
        )
        self.assertEqual(result.source_window.finality_status, "FINALIZED")

    def test_niftyfpi_missing_target_source_is_governed_exclusion_candidate(self):
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

        with self.assertRaises(TargetDateHistoryUnavailableCandidate) as raised:
            runner.build_history_for_symbol(
                "NIFTYFPI", history_days=5, end_date=date(2026, 8, 27)
            )

        evidence = raised.exception.evidence
        self.assertEqual(raised.exception.reason, "TARGET_DATE_HISTORY_UNAVAILABLE")
        self.assertEqual(evidence.target_daily_candle_count, 0)
        self.assertEqual(evidence.target_intraday_candle_count, 0)
        self.assertIn("2026-08-26", evidence.latest_daily_timestamp)
        self.assertIn("2026-08-26", evidence.latest_intraday_timestamp)

        self.assertTrue(kite.historical_data.called)
        self.assertEqual(kite.historical_data.call_count, 2)
        self.assertEqual(
            {call.kwargs["interval"] for call in kite.historical_data.call_args_list},
            {"day", "60minute"},
        )
        self.assertTrue(
            all(
                call.kwargs["instrument_token"] == 9200130
                for call in kite.historical_data.call_args_list
            )
        )

        manifest = build_strict_preparation_manifest(
            ["ABC", "NIFTYFPI"],
            {"ABC"},
            [raised.exception],
            end_date=date(2026, 8, 27),
        )
        exclusion = manifest["exclusions"][0]
        self.assertEqual(exclusion["reason"], "TARGET_DATE_HISTORY_UNAVAILABLE")
        validate_target_date_unavailable_exclusion(exclusion, date(2026, 8, 27))

    def test_missing_target_source_for_unallowlisted_symbol_fails_closed(self):
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
                            ["2026-08-26T00:00:00+05:30"], utc=True
                        ),
                        "volume": [1],
                    }
                ),
            },
            {
                "resolved_exchange": "NFO",
                "resolved_tradingsymbol": "OTHER26SEPFUT",
                "instrument_token": 99,
            },
        )
        with self.assertRaisesRegex(RuntimeError, "No completed market session"):
            runner.build_history_for_symbol(
                "OTHER", history_days=1, end_date=date(2026, 8, 27)
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

    def test_failure_logging_retains_exception_type_and_message(self):
        exception_type, message = exception_detail(
            RuntimeError("No completed market session found for OTHER on 2026-08-27.")
        )
        self.assertEqual(exception_type, "RuntimeError")
        self.assertEqual(
            message,
            "No completed market session found for OTHER on 2026-08-27.",
        )

    @patch.object(trend_sync, "persist_strict_trend_preparation", return_value=1)
    @patch.object(trend_sync, "get_engine", return_value=object())
    @patch.object(trend_sync, "EquityTrendHistoryRunner")
    @patch.object(trend_sync, "TrendIdentifierKiteAdapter")
    @patch.object(trend_sync, "KiteConnect")
    @patch.object(trend_sync, "get_kite_credentials", return_value=("key", "token"))
    def test_governed_target_date_exclusion_continues_without_synthetic_row(
        self,
        _credentials,
        _kite,
        adapter_cls,
        runner_cls,
        _engine,
        persist_preparation,
    ):
        adapter_cls.return_value.get_fo_universe_symbols.return_value = [
            "ABC",
            "NIFTYFPI",
        ]
        valid_result = strict_result()
        unavailable = TargetDateHistoryUnavailableCandidate(
            TargetDateHistoryUnavailableEvidence(
                symbol="NIFTYFPI",
                target_date=date(2026, 9, 8),
                exchange="NFO",
                tradingsymbol="NIFTYFPI26SEPFUT",
                instrument_token=9200130,
                required_interval="60minute",
                target_daily_candle_count=0,
                target_intraday_candle_count=0,
                latest_daily_timestamp="2026-09-07T00:00:00+05:30",
                latest_intraday_timestamp="2026-09-07T15:15:00+05:30",
            )
        )
        runner_cls.return_value.build_history_for_symbol.side_effect = [
            valid_result,
            unavailable,
        ]

        with patch.object(
            sys,
            "argv",
            ["sync", "OMK569", "--end-date", "2026-09-08", "--strict"],
        ):
            trend_sync.main()

        persisted_rows = persist_preparation.call_args.args[1]
        self.assertEqual([row["symbol"] for row in persisted_rows], ["ABC"])
        exclusions = persist_preparation.call_args.kwargs["exclusions"]
        self.assertEqual(len(exclusions), 1)
        self.assertEqual(exclusions[0]["symbol"], "NIFTYFPI")
        self.assertEqual(
            exclusions[0]["reason"], "TARGET_DATE_HISTORY_UNAVAILABLE"
        )

    @patch.object(trend_sync, "persist_strict_trend_preparation")
    @patch.object(trend_sync, "get_engine", return_value=object())
    @patch.object(trend_sync, "EquityTrendHistoryRunner")
    @patch.object(trend_sync, "TrendIdentifierKiteAdapter")
    @patch.object(trend_sync, "KiteConnect")
    @patch.object(trend_sync, "get_kite_credentials", return_value=("key", "token"))
    def test_unexpected_runtime_error_still_fails_strict_pipeline_with_message(
        self,
        _credentials,
        _kite,
        adapter_cls,
        runner_cls,
        _engine,
        persist_preparation,
    ):
        adapter_cls.return_value.get_fo_universe_symbols.return_value = ["OTHER"]
        runner_cls.return_value.build_history_for_symbol.side_effect = RuntimeError(
            "unexpected source failure"
        )
        output = io.StringIO()
        with patch.object(
            sys,
            "argv",
            ["sync", "OMK569", "--end-date", "2026-09-08", "--strict"],
        ):
            with redirect_stdout(output), self.assertRaises(SystemExit) as raised:
                trend_sync.main()
        self.assertEqual(raised.exception.code, 1)
        self.assertIn(
            "OTHER: RuntimeError: unexpected source failure", output.getvalue()
        )
        persist_preparation.assert_not_called()

    @patch.object(trend_sync, "persist_strict_trend_preparation")
    @patch.object(trend_sync, "get_engine", return_value=object())
    @patch.object(trend_sync, "EquityTrendHistoryRunner")
    @patch.object(trend_sync, "TrendIdentifierKiteAdapter")
    @patch.object(trend_sync, "KiteConnect")
    @patch.object(trend_sync, "get_kite_credentials", return_value=("key", "token"))
    def test_database_persistence_failure_still_fails_pipeline(
        self,
        _credentials,
        _kite,
        adapter_cls,
        runner_cls,
        _engine,
        persist_preparation,
    ):
        adapter_cls.return_value.get_fo_universe_symbols.return_value = ["ABC"]
        runner_cls.return_value.build_history_for_symbol.return_value = strict_result()
        persist_preparation.side_effect = RuntimeError("database unavailable")
        with patch.object(
            sys,
            "argv",
            ["sync", "OMK569", "--end-date", "2026-09-08", "--strict"],
        ):
            with self.assertRaisesRegex(RuntimeError, "database unavailable"):
                trend_sync.main()

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
        daily = pd.DataFrame(
            {
                "timestamp": pd.to_datetime(
                    ["2026-08-19T00:00:00+05:30", "2026-08-20T00:00:00+05:30"],
                    utc=True,
                ),
                "open": [99.0, 100.0],
                "high": [101.0, 102.0],
                "low": [98.0, 99.0],
                "close": [100.0, 101.0],
                "volume": [100, 110],
            }
        )
        equity.build_raw_bars_for_symbol_asof.return_value = (
            {"hourly": hourly, "daily": daily},
            {
                "resolved_exchange": "NSE",
                "resolved_tradingsymbol": "ABC",
                "instrument_token": 1,
            },
        )
        equity.evaluate_raw_bars_asof.return_value = {
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
                "daily": pd.DataFrame(
                    {
                        "timestamp": pd.to_datetime(
                            ["2026-08-21T00:00:00+05:30"], utc=True
                        ),
                        "open": [99.0],
                        "high": [101.0],
                        "low": [98.0],
                        "close": [100.0],
                        "volume": [100],
                    }
                ),
            },
            {
                "resolved_exchange": "NSE",
                "resolved_tradingsymbol": "ABC",
                "instrument_token": 1,
            },
        )
        with self.assertRaisesRegex(RuntimeError, "No completed market session"):
            runner.build_history_for_symbol(
                "ABC", history_days=1, end_date=date(2026, 8, 22)
            )

    def test_exact_contract_mode_cannot_fall_back(self):
        adapter = CapturingDbAdapter(
            engine=object(),
            run_date=date(2026, 8, 20),
            user_id="XJ1877",
            require_exact_contract_snapshot=True,
        )
        inputs = adapter.build_all()
        self.assertEqual(len(inputs), 1)
        self.assertIn("trade_date <= :run_date", adapter.sql[0])
        self.assertIn("ROW_NUMBER() OVER", adapter.sql[0])
        self.assertIn("PARTITION BY history.symbol", adapter.sql[0])
        self.assertIn("trade_date = :run_date", adapter.sql[0])
        self.assertIn("history.user_id = :user_id", adapter.sql[0])
        self.assertIn("user_id = :user_id", adapter.sql[1])
        self.assertIn("selection_date = :run_date", adapter.sql[1])
        self.assertNotIn("selection_date <= :run_date", adapter.sql[1])

    def test_ordinary_read_retains_intentional_snapshot_fallback(self):
        adapter = CapturingDbAdapter(
            engine=object(),
            run_date=date(2026, 8, 20),
            user_id="OMK569",
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
        self.assertTrue(
            trend_parser().parse_args(
                [
                    "OMK569",
                    "--end-date",
                    "2026-08-31",
                    "--strict",
                    "--restart-unverified-completed-run",
                ]
            ).restart_unverified_completed_run
        )

    @patch.object(trend_sync, "get_engine")
    @patch.object(trend_sync, "EquityTrendHistoryRunner")
    @patch.object(trend_sync, "TrendIdentifierKiteAdapter")
    @patch.object(trend_sync, "KiteConnect")
    @patch.object(trend_sync, "get_kite_credentials", return_value=("key", "token"))
    def test_trend_token_exception_aborts_universe_immediately(
        self, _credentials, _kite, adapter_cls, runner_cls, _engine
    ):
        adapter_cls.return_value.get_fo_universe_symbols.return_value = ["A", "B"]
        runner_cls.return_value.build_history_for_symbol.side_effect = TokenException(
            "expired"
        )
        with patch.object(sys, "argv", ["sync", "OMK569"]):
            with self.assertRaises(SystemExit) as raised:
                trend_sync.main()
        self.assertEqual(raised.exception.code, 1)
        self.assertEqual(
            runner_cls.return_value.build_history_for_symbol.call_count,
            1,
        )

    @patch.object(contract_sync, "get_engine")
    @patch.object(contract_sync, "TrendIdentifierKiteAdapter")
    @patch.object(contract_sync, "KiteConnect")
    @patch.object(contract_sync, "get_kite_credentials", return_value=("key", "token"))
    def test_contract_token_exception_aborts_universe_immediately(
        self, _credentials, _kite, adapter_cls, _engine
    ):
        adapter = adapter_cls.return_value
        adapter.get_fo_universe_symbols.return_value = ["A", "B"]
        adapter._build_latest_payload.side_effect = TokenException("expired")
        with patch.object(sys, "argv", ["sync", "OMK569"]):
            with self.assertRaises(SystemExit) as raised:
                contract_sync.main()
        self.assertEqual(raised.exception.code, 1)
        self.assertEqual(adapter._build_latest_payload.call_count, 1)

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

    def test_per_symbol_session_patterns_reject_duplicate_account_session(self):
        rows = [
            ("ABC", date(2026, 8, day))
            for day in (14, 17, 18, 20, 20)
        ]
        with self.assertRaisesRegex(RuntimeError, "duplicate"):
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

    def test_target_date_unavailable_exclusion_is_symbol_scoped(self):
        evidence = TargetDateHistoryUnavailableEvidence(
            symbol="OTHER",
            target_date=date(2026, 9, 8),
            exchange="NFO",
            tradingsymbol="OTHER26SEPFUT",
            instrument_token=99,
            required_interval="60minute",
            target_daily_candle_count=0,
            target_intraday_candle_count=0,
            latest_daily_timestamp="2026-09-07T00:00:00+05:30",
            latest_intraday_timestamp="2026-09-07T15:15:00+05:30",
        )
        exclusion = build_strict_preparation_manifest(
            ["ABC", "OTHER"],
            {"ABC"},
            [TargetDateHistoryUnavailableCandidate(evidence)],
            end_date=date(2026, 9, 8),
        )["exclusions"][0]
        with self.assertRaisesRegex(RuntimeError, "not governed"):
            validate_target_date_unavailable_exclusion(exclusion, date(2026, 9, 8))

    def test_strict_preparation_persists_rows_and_manifest_atomically(self):
        connection = MagicMock()
        connection.execute.return_value.fetchone.return_value = (7,)
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
            acquisition=acquisition_fixture(),
        )

        self.assertEqual(written, 1)
        self.assertEqual(connection.execute.call_count, 3)
        manifest_params = connection.execute.call_args_list[2].args[1]
        self.assertEqual(manifest_params["requested_symbols_count"], 2)
        self.assertEqual(manifest_params["prepared_symbols_count"], 1)
        self.assertIn("NO_TRADING_ACTIVITY", manifest_params["input_exclusions_json"])
        self.assertFalse(manifest_params["allow_unverified_completed_restart"])

    def test_recovery_restart_is_limited_to_completed_runs_without_provenance(self):
        sql = str(PREPARE_STRATEGY_RUN_SQL)
        self.assertIn(":allow_unverified_completed_restart", sql)
        self.assertIn("requested_symbols_count IS NULL", sql)
        self.assertIn("prepared_symbols_count IS NULL", sql)
        self.assertIn("evaluated_symbols_count IS NULL", sql)
        self.assertIn("input_exclusions_json IS NULL", sql)

        connection = MagicMock()
        connection.execute.return_value.fetchone.return_value = (87,)
        transaction = MagicMock()
        transaction.__enter__.return_value = connection
        engine = MagicMock()
        engine.begin.return_value = transaction
        persist_strict_trend_preparation(
            engine,
            [],
            run_date=date(2026, 8, 31),
            generated_by_user_id="OMK569",
            requested_symbols_count=0,
            prepared_symbols_count=0,
            exclusions=[],
            acquisition=acquisition_fixture(),
            allow_unverified_completed_restart=True,
        )
        params = connection.execute.call_args_list[1].args[1]
        self.assertTrue(params["allow_unverified_completed_restart"])

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
