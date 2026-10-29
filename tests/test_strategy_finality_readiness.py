"""No-network coverage of the finality checker's retry/diagnostic contract."""

from contextlib import redirect_stdout
import io
import unittest
from unittest.mock import patch

from engines.strategy_deterministic_engine.scripts import (
    check_strategy_finality_readiness as checker,
)
from services.nse_final_close import NseFinalCloseUnavailable


class StrategyFinalityReadinessTests(unittest.TestCase):
    def test_transport_failure_is_retryable_then_reconciliation_can_succeed(self):
        failure = NseFinalCloseUnavailable(
            "Official NSE CM final evidence is unavailable. "
            "cause=HTTPError http_status=403"
        )
        with (
            patch("sys.argv", ["readiness", "XJ1877", "--run-date", "2026-09-21"]),
            patch.object(checker, "get_kite_credentials", return_value=("test-key", "test-token")),
            patch.object(checker, "KiteConnect"),
            patch.object(checker, "TrendIdentifierKiteAdapter") as adapter,
            patch.object(checker, "NseFinalCloseProvider") as provider,
            patch.object(checker, "EquityTrendHistoryRunner") as runner,
        ):
            adapter.return_value.get_fo_universe_symbols.return_value = ["LICI", "TRENT"]
            provider.return_value.fetch.side_effect = failure
            pending_output = io.StringIO()
            with redirect_stdout(pending_output), self.assertRaises(SystemExit) as raised:
                checker.main()
            self.assertEqual(raised.exception.code, 75)
            self.assertIn("FINALITY_READINESS=PENDING", pending_output.getvalue())
            self.assertIn("cause=HTTPError http_status=403", pending_output.getvalue())
            self.assertNotIn("test-token", pending_output.getvalue())
            runner.assert_not_called()

            provider.return_value.fetch.side_effect = None
            provider.return_value.fetch.return_value.filter_current_universe.side_effect = lambda symbols: list(symbols)
            provider.return_value.fetch.return_value.nearest_index_future.return_value = None
            ready_output = io.StringIO()
            with redirect_stdout(ready_output):
                checker.main()
            self.assertIn("FINALITY_READINESS=READY", ready_output.getvalue())
            self.assertEqual(runner.return_value.check_target_session_finality.call_count, 2)
            self.assertEqual(
                [call.args[0] for call in runner.return_value.check_target_session_finality.call_args_list],
                ["LICI", "TRENT"],
            )
