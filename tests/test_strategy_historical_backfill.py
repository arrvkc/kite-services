from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, timezone
from decimal import Decimal
from contextlib import redirect_stdout
import io
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock
from unittest.mock import patch
import sys

import pandas as pd
from kiteconnect.exceptions import TokenException
from sqlalchemy import create_engine, text

from services.nse_final_close import OfficialFinalCloseEvidence

from engines.strategy_deterministic_engine.adapters.trend_identifier_adapter import (
    TrendIdentifierKiteAdapter,
)
from engines.strategy_deterministic_engine.adapters.trend_identifier_db_adapter import (
    TrendIdentifierDbAdapter,
)
from engines.strategy_deterministic_engine.db.upserts import (
    DELETE_STRATEGY_BATCH_RESULTS_FOR_DATE_SQL,
    PREPARE_STRATEGY_RUN_SQL,
    persist_pending_historical_acquisition,
    persist_strict_trend_preparation,
    validate_historical_chunk_manifests,
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
from engines.trend_identifier.trend_identifier.runners.equity_trend_runner import (
    InstrumentMatch,
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


def official_close(
    symbol: str, target_date: date, close: object
) -> OfficialFinalCloseEvidence:
    return OfficialFinalCloseEvidence(
        session_date=target_date,
        exchange="NSE",
        tradingsymbol=symbol,
        instrument_kind="NSE_CM_EQ_FINAL",
        close=Decimal(str(close)),
        source_name=(
            "BhavCopy_NSE_CM_0_0_0_"
            + target_date.strftime("%Y%m%d")
            + "_F_0000.csv.zip"
        ),
        source_sha256="f" * 64,
        retrieved_at=datetime(2026, 5, 22, 11, tzinfo=timezone.utc),
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
        "chunks": [],
        "snapshot_chunks": [],
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
    def test_minimal_finality_readiness_matches_official_close(self):
        runner = EquityTrendHistoryRunner.__new__(EquityTrendHistoryRunner)
        runner.exchange = "NSE"
        runner.equity_runner = MagicMock()
        runner.equity_runner.resolve_instrument_asof.return_value = InstrumentMatch(
            instrument_token=123,
            exchange="NSE",
            tradingsymbol="ABC",
            name="ABC",
            segment="NSE",
            instrument_type="EQ",
        )
        runner.equity_runner.fetch_historical_data.return_value = pd.DataFrame(
            [{
                "timestamp": pd.Timestamp("2026-09-08T00:00:00+05:30"),
                "open": 99,
                "high": 105,
                "low": 98,
                "close": 104,
                "volume": 1000,
            }]
        )
        evidence = runner.check_target_session_finality(
            "ABC",
            date(2026, 9, 8),
            lambda _instrument, target: official_close("ABC", target, 104),
        )
        self.assertEqual(evidence.close, Decimal("104"))
        runner.equity_runner.fetch_historical_data.assert_called_once()
        kwargs = runner.equity_runner.fetch_historical_data.call_args.kwargs
        self.assertEqual(kwargs["interval"], "day")
        self.assertFalse(kwargs["continuous"])

        with self.assertRaisesRegex(
            TargetSessionNotFinalized, "does not match official NSE final"
        ):
            runner.check_target_session_finality(
                "ABC",
                date(2026, 9, 8),
                lambda _instrument, target: official_close("ABC", target, 104.05),
            )

    def test_minimal_readiness_preserves_governed_missing_target_exclusion(self):
        runner = EquityTrendHistoryRunner.__new__(EquityTrendHistoryRunner)
        runner.exchange = "NSE"
        runner.equity_runner = MagicMock()
        runner.equity_runner.resolve_instrument_asof.return_value = InstrumentMatch(
            instrument_token=123,
            exchange="NFO",
            tradingsymbol="NIFTYFPI26SEPFUT",
            name="NIFTYFPI",
            segment="NFO-FUT",
            instrument_type="FUT",
        )
        runner.equity_runner.fetch_historical_data.side_effect = [
            pd.DataFrame(),
            pd.DataFrame(),
        ]
        self.assertIsNone(
            runner.check_target_session_finality(
                "NIFTYFPI",
                date(2026, 9, 8),
                MagicMock(),
            )
        )
        self.assertEqual(
            [
                call.kwargs["interval"]
                for call in (
                    runner.equity_runner.fetch_historical_data.call_args_list
                )
            ],
            ["day", "60minute"],
        )

    def _coherent_history_result(
        self, symbol, closes, *, retrieved_at=None, official_value=None
    ):
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
                "kite_instrument_type": "EQ",
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
            final_close_resolver=lambda _instrument, target_date: official_close(
                symbol,
                target_date,
                closes[-1] if official_value is None else official_value,
            ),
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
        self.assertNotEqual(
            first_evidence["chunks"][0]["id"],
            revised_evidence["chunks"][0]["id"],
        )

    def test_identical_content_reuses_chunks_with_distinct_acquisitions(self):
        result, _ = self._coherent_history_result(
            "NORMAL", [100.0, 101.0, 102.0]
        )
        first = build_acquisition_evidence(
            "acquisition-1", SimpleNamespace(user_id="XJ1877"), result
        )
        second = build_acquisition_evidence(
            "acquisition-2", SimpleNamespace(user_id="XJ1877"), result
        )
        self.assertNotEqual(first["snapshots"][0]["id"], second["snapshots"][0]["id"])
        self.assertEqual(first["chunks"][0]["id"], second["chunks"][0]["id"])
        self.assertEqual(first["bars"][0]["chunk_id"], second["bars"][0]["chunk_id"])
        self.assertNotIn("snapshot_id", first["bars"][0])

    def test_chunk_identity_cannot_cross_symbol_or_interval(self):
        result, _ = self._coherent_history_result(
            "NORMAL", [100.0, 101.0, 102.0]
        )
        normal = build_acquisition_evidence(
            "acquisition-1", SimpleNamespace(user_id="XJ1877"), result
        )
        other_symbol = replace(result, symbol="OTHER", tradingsymbol="OTHER")
        other = build_acquisition_evidence(
            "acquisition-2", SimpleNamespace(user_id="XJ1877"), other_symbol
        )
        self.assertNotEqual(normal["chunks"][0]["id"], other["chunks"][0]["id"])
        self.assertNotEqual(normal["chunks"][0]["id"], normal["chunks"][1]["id"])

    def _with_source_frames(self, result, daily, hourly):
        window = replace(
            result.source_window,
            daily=daily,
            hourly=hourly,
            daily_content_hash=bars_content_hash(daily),
            hourly_content_hash=bars_content_hash(hourly),
        )
        return replace(result, source_window=window)

    def test_daily_revision_replaces_only_affected_calendar_month_chunk(self):
        result, _ = self._coherent_history_result("NORMAL", [100.0] * 5)
        dates = pd.date_range("2026-05-04", "2026-06-12", freq="B", tz="Asia/Kolkata")
        daily = pd.DataFrame({
            "timestamp": dates,
            "open": [100.0] * len(dates), "high": [102.0] * len(dates),
            "low": [98.0] * len(dates), "close": [100.0] * len(dates),
            "volume": [1000] * len(dates),
        })
        first_result = self._with_source_frames(result, daily, result.source_window.hourly)
        revised_daily = daily.copy()
        revised_daily.loc[revised_daily["timestamp"].dt.month == 5, "close"] = 99.5
        revised_result = self._with_source_frames(
            result, revised_daily, result.source_window.hourly
        )
        first = build_acquisition_evidence("a1", SimpleNamespace(user_id="XJ1877"), first_result)
        revised = build_acquisition_evidence("a2", SimpleNamespace(user_id="XJ1877"), revised_result)
        first_ids = {(c["interval"], c["chunk_key"]): c["id"] for c in first["chunks"]}
        revised_ids = {(c["interval"], c["chunk_key"]): c["id"] for c in revised["chunks"]}
        self.assertNotEqual(first_ids[("day", "2026-05")], revised_ids[("day", "2026-05")])
        self.assertEqual(first_ids[("day", "2026-06")], revised_ids[("day", "2026-06")])
        self.assertEqual(
            {k: v for k, v in first_ids.items() if k[0] == "60minute"},
            {k: v for k, v in revised_ids.items() if k[0] == "60minute"},
        )

    def test_intraday_revision_replaces_only_affected_iso_week_chunk(self):
        result, _ = self._coherent_history_result("NORMAL", [100.0] * 10)
        hourly = result.source_window.hourly.copy()
        first_result = self._with_source_frames(result, result.source_window.daily, hourly)
        revised_hourly = hourly.copy()
        first_week = revised_hourly["timestamp"].iloc[0].isocalendar().week
        mask = revised_hourly["timestamp"].map(lambda value: value.isocalendar().week == first_week)
        revised_hourly.loc[mask, "close"] = 99.5
        revised_result = self._with_source_frames(
            result, result.source_window.daily, revised_hourly
        )
        first = build_acquisition_evidence("a1", SimpleNamespace(user_id="XJ1877"), first_result)
        revised = build_acquisition_evidence("a2", SimpleNamespace(user_id="XJ1877"), revised_result)
        first_ids = {(c["interval"], c["chunk_key"]): c["id"] for c in first["chunks"]}
        revised_ids = {(c["interval"], c["chunk_key"]): c["id"] for c in revised["chunks"]}
        changed = [key for key in first_ids if first_ids[key] != revised_ids[key]]
        self.assertEqual(changed, [("60minute", f"2026-W{first_week:02d}")])

    def test_snapshot_reconstruction_rejects_missing_and_overlapping_references(self):
        bars = [{
            "timestamp": "2026-05-18T00:00:00+05:30", "open": "1",
            "high": "1", "low": "1", "close": "1", "volume": "1", "oi": None,
        }]
        valid = [{
            "ordinal": 0, "chunk_key": "2026-05", "chunk_id": "a",
            "interval": "day", "bars": bars,
        }]
        self.assertEqual(trend_sync.reconstruct_snapshot_bars(valid), bars)
        with self.assertRaisesRegex(RuntimeError, "ordinals are incomplete"):
            trend_sync.reconstruct_snapshot_bars([{**valid[0], "ordinal": 1}])
        with self.assertRaisesRegex(RuntimeError, "falls outside its chunk boundary"):
            trend_sync.reconstruct_snapshot_bars(valid + [{
                **valid[0], "ordinal": 1, "chunk_key": "2026-06", "chunk_id": "b",
            }])
        with self.assertRaisesRegex(RuntimeError, "chunk keys overlap"):
            trend_sync.reconstruct_snapshot_bars(valid + [{
                **valid[0], "ordinal": 1, "chunk_id": "b",
            }])

    def test_manifest_validation_rejects_missing_and_cross_symbol_chunks(self):
        result, _ = self._coherent_history_result("NORMAL", [100.0] * 5)
        evidence = build_acquisition_evidence(
            "a1", SimpleNamespace(user_id="XJ1877"), result
        )
        acquisition = {
            "chunks": evidence["chunks"], "snapshots": evidence["snapshots"],
            "snapshot_chunks": evidence["snapshot_chunks"],
        }
        validate_historical_chunk_manifests(acquisition)
        missing = {**acquisition, "snapshot_chunks": acquisition["snapshot_chunks"][1:]}
        with self.assertRaisesRegex(RuntimeError, "has no chunk references"):
            validate_historical_chunk_manifests(missing)
        crossed_chunks = [dict(item) for item in acquisition["chunks"]]
        crossed_chunks[0]["symbol"] = "OTHER"
        with self.assertRaisesRegex(RuntimeError, "cross-identity chunk"):
            validate_historical_chunk_manifests({**acquisition, "chunks": crossed_chunks})

    def test_identical_content_is_physically_stored_once_for_two_acquisitions(self):
        engine = create_engine("sqlite://")
        with engine.begin() as connection:
            connection.execute(text("""
                CREATE TABLE historical_price_acquisitions (
                    id VARCHAR(36) PRIMARY KEY, source_account VARCHAR NOT NULL,
                    target_session_date DATE NOT NULL, retrieved_at TIMESTAMP NOT NULL,
                    source VARCHAR NOT NULL DEFAULT 'ZERODHA_HISTORICAL_DATA',
                    status VARCHAR NOT NULL, finality_status VARCHAR NOT NULL,
                    content_hash VARCHAR NOT NULL
                )
            """))
            connection.execute(text("""
                CREATE TABLE historical_price_chunks (
                    id VARCHAR(64) PRIMARY KEY, source VARCHAR NOT NULL,
                    symbol VARCHAR NOT NULL, exchange VARCHAR NOT NULL,
                    tradingsymbol VARCHAR NOT NULL, instrument_token BIGINT NOT NULL,
                    interval VARCHAR NOT NULL, chunk_key VARCHAR NOT NULL,
                    covered_from TIMESTAMP NOT NULL,
                    covered_to TIMESTAMP NOT NULL, bar_count INTEGER NOT NULL,
                    content_hash VARCHAR NOT NULL
                )
            """))
            connection.execute(text("""
                CREATE TABLE historical_price_snapshots (
                    id VARCHAR(36) PRIMARY KEY, acquisition_id VARCHAR(36) NOT NULL,
                    source_account VARCHAR NOT NULL,
                    symbol VARCHAR NOT NULL, exchange VARCHAR NOT NULL,
                    tradingsymbol VARCHAR NOT NULL, instrument_token BIGINT NOT NULL,
                    interval VARCHAR NOT NULL, target_session_date DATE NOT NULL,
                    covered_from TIMESTAMP NOT NULL, covered_to TIMESTAMP NOT NULL,
                    retrieved_at TIMESTAMP NOT NULL, bar_count INTEGER NOT NULL,
                    content_hash VARCHAR NOT NULL, validation_status VARCHAR NOT NULL,
                    finality_source VARCHAR, finality_source_key VARCHAR,
                    finality_source_hash VARCHAR, finality_close NUMERIC,
                    finality_retrieved_at TIMESTAMP
                )
            """))
            connection.execute(text("""
                CREATE TABLE historical_price_snapshot_chunks (
                    snapshot_id VARCHAR(36) NOT NULL, chunk_id VARCHAR(64) NOT NULL,
                    ordinal INTEGER NOT NULL, chunk_key VARCHAR NOT NULL,
                    PRIMARY KEY(snapshot_id, ordinal),
                    UNIQUE(snapshot_id, chunk_key), UNIQUE(snapshot_id, chunk_id)
                )
            """))
            connection.execute(text("""
                CREATE TABLE historical_price_bars (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, chunk_id VARCHAR(64) NOT NULL,
                    observed_at TIMESTAMP NOT NULL, session_date DATE NOT NULL,
                    open NUMERIC NOT NULL, high NUMERIC NOT NULL, low NUMERIC NOT NULL,
                    close NUMERIC NOT NULL, volume NUMERIC NOT NULL, oi NUMERIC,
                    UNIQUE(chunk_id, observed_at)
                )
            """))

        result, _ = self._coherent_history_result(
            "NORMAL", [100.0, 101.0, 102.0]
        )
        for acquisition_id in ("acquisition-1", "acquisition-2"):
            evidence = build_acquisition_evidence(
                acquisition_id, SimpleNamespace(user_id="XJ1877"), result
            )
            for snapshot in evidence["snapshots"]:
                if snapshot["finality_close"] is not None:
                    snapshot["finality_close"] = str(snapshot["finality_close"])
            persist_pending_historical_acquisition(
                engine,
                acquisition={
                    "id": acquisition_id,
                    "source_account": "XJ1877",
                    "target_session_date": result.source_window.target_session_date,
                    "retrieved_at": result.source_window.retrieved_at,
                    "status": "PENDING",
                    "finality_status": "PENDING_FINALITY",
                    "content_hash": acquisition_content_hash(evidence["snapshots"]),
                    "chunks": evidence["chunks"],
                    "snapshot_chunks": evidence["snapshot_chunks"],
                    "snapshots": evidence["snapshots"],
                    "bars": evidence["bars"],
                },
            )

        with engine.connect() as connection:
            self.assertEqual(
                connection.execute(
                    text("SELECT count(*) FROM historical_price_acquisitions")
                ).scalar_one(),
                2,
            )
            self.assertEqual(
                connection.execute(
                    text("SELECT count(*) FROM historical_price_chunks")
                ).scalar_one(),
                2,
            )
            self.assertEqual(
                connection.execute(
                    text("SELECT count(*) FROM historical_price_snapshots")
                ).scalar_one(),
                4,
            )
            self.assertEqual(
                connection.execute(
                    text("SELECT count(*) FROM historical_price_snapshot_chunks")
                ).scalar_one(),
                len(evidence["snapshot_chunks"]) * 2,
            )
            self.assertEqual(
                connection.execute(
                    text("SELECT count(*) FROM historical_price_bars")
                ).scalar_one(),
                len(evidence["bars"]),
            )

    def test_same_day_target_is_final_only_when_official_nse_close_matches(self):
        closes = [100.0, 101.0, 102.0, 103.0, 104.0]
        result, _ = self._coherent_history_result(
            "ABC",
            closes,
            retrieved_at=datetime(2026, 5, 22, 10, 31, tzinfo=timezone.utc),
        )
        self.assertEqual(result.source_window.finality_status, "FINALIZED")
        self.assertEqual(result.source_window.finality_close, Decimal("104.0"))
        self.assertEqual(result.source_window.finality_source, "NSE_CM_EQ_FINAL")
        evidence = build_acquisition_evidence(
            "same-day-final", SimpleNamespace(user_id="XJ1877"), result
        )
        daily_snapshot = next(
            item for item in evidence["snapshots"] if item["interval"] == "day"
        )
        self.assertEqual(daily_snapshot["finality_source"], "NSE_CM_EQ_FINAL")
        self.assertEqual(daily_snapshot["finality_close"], Decimal("104.0"))
        self.assertEqual(daily_snapshot["finality_source_hash"], "f" * 64)

        with self.assertRaisesRegex(
            TargetSessionNotFinalized, "does not match official NSE final"
        ) as mismatch:
            self._coherent_history_result(
                "ABC",
                closes,
                retrieved_at=datetime(2026, 5, 22, 10, 31, tzinfo=timezone.utc),
                official_value=Decimal("104.05"),
            )
        self.assertEqual(
            mismatch.exception.source_window.finality_status,
            "PENDING_FINALITY",
        )

    def test_no_official_final_close_fails_even_on_later_calendar_date(self):
        runner = EquityTrendHistoryRunner.__new__(EquityTrendHistoryRunner)
        runner.exchange = "NSE"
        result, equity = self._coherent_history_result(
            "ABC", [100.0, 101.0, 102.0]
        )
        runner.equity_runner = equity
        with self.assertRaisesRegex(
            TargetSessionNotFinalized, "no official NSE final-close evidence"
        ):
            runner.build_history_for_symbol(
                "ABC",
                history_days=3,
                end_date=result.source_window.target_session_date,
                retrieved_at=datetime(2026, 6, 30, tzinfo=timezone.utc),
            )

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
        with self.assertRaisesRegex(
            TargetSessionNotFinalized, "no completed intraday evidence"
        ):
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
        ), patch.object(trend_sync, "NseFinalCloseProvider"):
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
        ), patch.object(trend_sync, "NseFinalCloseProvider"):
            with redirect_stdout(output), self.assertRaises(SystemExit) as raised:
                trend_sync.main()
        self.assertEqual(raised.exception.code, 1)
        self.assertIn(
            "OTHER: RuntimeError: unexpected source failure", output.getvalue()
        )
        persist_preparation.assert_not_called()

    @patch.object(trend_sync, "persist_pending_historical_acquisition")
    @patch.object(trend_sync, "persist_strict_trend_preparation")
    @patch.object(trend_sync, "get_engine", return_value=object())
    @patch.object(trend_sync, "EquityTrendHistoryRunner")
    @patch.object(trend_sync, "TrendIdentifierKiteAdapter")
    @patch.object(trend_sync, "KiteConnect")
    @patch.object(trend_sync, "get_kite_credentials", return_value=("key", "token"))
    def test_unreconciled_final_close_persists_pending_evidence_without_publication(
        self,
        _credentials,
        _kite,
        adapter_cls,
        runner_cls,
        _engine,
        persist_preparation,
        persist_pending,
    ):
        adapter_cls.return_value.get_fo_universe_symbols.return_value = ["ABC"]
        window = replace(
            coherent_source_window(),
            finality_status="PENDING_FINALITY",
        )
        runner_cls.return_value.build_history_for_symbol.side_effect = (
            TargetSessionNotFinalized(
                "ABC has not reconciled to NSE final.",
                source_window=window,
            )
        )
        with patch.object(
            sys,
            "argv",
            ["sync", "XJ1877", "--end-date", "2026-09-08", "--strict"],
        ), patch.object(trend_sync, "NseFinalCloseProvider"):
            with self.assertRaises(SystemExit) as raised:
                trend_sync.main()
        self.assertEqual(raised.exception.code, 75)
        persist_preparation.assert_not_called()
        persisted = persist_pending.call_args.kwargs["acquisition"]
        self.assertEqual(persisted["status"], "PENDING")
        self.assertEqual(persisted["finality_status"], "PENDING_FINALITY")
        self.assertEqual(len(persisted["chunks"]), 2)
        self.assertEqual(len(persisted["snapshots"]), 2)

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
        ), patch.object(trend_sync, "NseFinalCloseProvider"):
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
                "kite_instrument_type": "EQ",
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
            "ABC",
            history_days=2,
            end_date=date(2026, 8, 20),
            final_close_resolver=lambda _instrument, target_date: official_close(
                "ABC", target_date, 101.0
            ),
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
