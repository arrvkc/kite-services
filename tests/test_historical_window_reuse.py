from copy import deepcopy
from dataclasses import replace
from datetime import date
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock

import test_strategy_historical_backfill as fixtures
from engines.strategy_deterministic_engine.db.historical_windows import load_finalized_source_window
from engines.strategy_deterministic_engine.scripts.sync_trend_history_fo_universe_to_db import build_acquisition_evidence
from engines.trend_identifier.trend_identifier.runners.equity_trend_history_runner import EquityTrendHistoryRunner


class RetainedHistoricalWindowTests(unittest.TestCase):
    def fixture(self, mutation=None):
        result, _ = fixtures.HistoricalBackfillContractTests()._coherent_history_result("NIFTY", [100., 101., 102., 103., 104.])
        window = replace(result.source_window, exchange="NFO", tradingsymbol="NIFTY26MAYFUT", finality_source="NSE_FO_FUT_FINAL")
        result = replace(result, exchange="NFO", tradingsymbol="NIFTY26MAYFUT", source_window=window)
        evidence = build_acquisition_evidence("retained-acquisition", SimpleNamespace(user_id="XJ1877"), result)
        daily, hourly = evidence["snapshots"]
        daily = {**daily, "hourly_snapshot_id": hourly["id"]}
        if mutation == "account": hourly["source_account"] = "OTHER"
        if mutation == "acquisition": hourly["acquisition_id"] = "OTHER"
        responses = []
        def response(**kwargs):
            result = MagicMock()
            for key, value in kwargs.items(): getattr(result.mappings.return_value, key).return_value = value
            return result
        responses.extend([response(first=daily), response(one=hourly)])
        for snapshot in [daily, hourly]:
            chunks = []
            bar_sets = []
            for ref in evidence["snapshot_chunks"]:
                if ref["snapshot_id"] != snapshot["id"]: continue
                chunk = deepcopy(next(item for item in evidence["chunks"] if item["id"] == ref["chunk_id"]))
                chunk.update(ordinal=ref["ordinal"], snapshot_id=ref["snapshot_id"], reference_key=ref["chunk_key"])
                if mutation == "ordinal": chunk["ordinal"] += 1
                chunks.append(chunk)
                bars = [{"timestamp": item["observed_at"], **{key: item[key] for key in ["open", "high", "low", "close", "volume", "oi"]}}
                        for item in evidence["bars"] if item["chunk_id"] == chunk["id"]]
                if mutation == "bars": bars[0]["close"] = 999.
                bar_sets.append(bars)
            responses.append(response(all=chunks))
            responses.extend(response(all=bars) for bars in bar_sets)
        connection = MagicMock()
        connection.dialect.name = "sqlite"
        connection.execute.side_effect = responses
        engine = MagicMock()
        engine.connect.return_value.__enter__.return_value = connection
        return engine, connection, window

    def test_complete_window_reused_with_original_time_and_hashes(self):
        engine, connection, original = self.fixture()
        window = load_finalized_source_window(engine, "XJ1877", "NIFTY", date(2026, 5, 22), "NIFTY26MAYFUT")
        self.assertEqual(window.daily_content_hash, original.daily_content_hash)
        self.assertEqual(window.hourly_content_hash, original.hourly_content_hash)
        self.assertEqual(window.retrieved_at, original.retrieved_at)
        parameters = connection.execute.call_args_list[0].args[1]
        self.assertEqual(parameters["account"], "XJ1877")
        self.assertEqual(parameters["target"], date(2026, 5, 22))
        self.assertEqual(parameters["contract"], "NIFTY26MAYFUT")

    def test_no_complete_pair_returns_unavailable(self):
        engine, connection, _ = self.fixture()
        empty = MagicMock()
        empty.mappings.return_value.first.return_value = None
        connection.execute.side_effect = [empty]
        self.assertIsNone(load_finalized_source_window(engine, "XJ1877", "NIFTY", date(2026, 5, 22), "NIFTY26MAYFUT"))

    def test_mixed_identity_corrupt_bars_and_incomplete_manifests_fail_closed(self):
        for mutation in ["account", "acquisition", "bars", "ordinal"]:
            with self.subTest(mutation=mutation):
                engine, _, _ = self.fixture(mutation)
                with self.assertRaises(RuntimeError):
                    load_finalized_source_window(engine, "XJ1877", "NIFTY", date(2026, 5, 22), "NIFTY26MAYFUT")

    def test_retained_bars_are_reconciled_again_without_fetching_live_contract(self):
        from test_strategy_historical_backfill import official_close
        engine, _, _ = self.fixture()
        window = load_finalized_source_window(engine, "XJ1877", "NIFTY", date(2026, 5, 22), "NIFTY26MAYFUT")
        runner = EquityTrendHistoryRunner.__new__(EquityTrendHistoryRunner)
        runner.equity_runner = MagicMock()
        runner.equity_runner.evaluate_raw_bars_asof.side_effect = lambda **kwargs: {
            "symbol": "NIFTY", "exchange": "NFO", "tradingsymbol": window.tradingsymbol,
            "instrument_token": window.instrument_token,
            "payload": {"label": "UP", "confidence": .8, "aggregate_score": .5, "internal_state": "TRENDING"},
        }
        result = runner.build_history_for_symbol("NIFTY", 5, end_date=date(2026, 5, 22), source_window=window,
            final_close_resolver=lambda metadata, day: official_close("NIFTY", day, 104))
        self.assertEqual(len(result.history), 5)
        self.assertEqual(result.source_window.retrieved_at, window.retrieved_at)
        runner.equity_runner.build_raw_bars_for_symbol_asof.assert_not_called()
