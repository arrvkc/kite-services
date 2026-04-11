#!/usr/bin/env python3
import unittest
from engines.stop_engine.stop_computation_engine import StopComputationConfig, compute_deterministic_stop_eod

def make_base_candles():
    closes = [100,101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,121,122,123,124,125,126,127,128,129,130,131,132,133,134]
    candles = []
    for i, close in enumerate(closes):
        candles.append({"date": f"2026-01-{i+1:02d}", "open": float(close - 1), "high": float(close + 1), "low": float(close - 2), "close": float(close)})
    return candles

class TestDeterministicStopEngine(unittest.TestCase):
    def setUp(self):
        self.config = StopComputationConfig()

    def test_long_stop_is_below_current_price(self):
        candles = make_base_candles()
        result = compute_deterministic_stop_eod(candles=candles, side="LONG", tick_size=0.05, entry_price=120.0, previous_trigger_price=None, config=self.config)
        self.assertLess(result["trigger_price"], result["current_price_reference"])

    def test_short_stop_is_above_current_price(self):
        candles = make_base_candles()
        result = compute_deterministic_stop_eod(candles=candles, side="SHORT", tick_size=0.05, entry_price=120.0, previous_trigger_price=None, config=self.config)
        self.assertGreater(result["trigger_price"], result["current_price_reference"])

    def test_long_monotonicity(self):
        candles = make_base_candles()
        result = compute_deterministic_stop_eod(candles=candles, side="LONG", tick_size=0.05, entry_price=120.0, previous_trigger_price=126.25, config=self.config)
        self.assertGreaterEqual(result["trigger_price"], 126.25)

    def test_short_monotonicity(self):
        candles = make_base_candles()
        result = compute_deterministic_stop_eod(candles=candles, side="SHORT", tick_size=0.05, entry_price=120.0, previous_trigger_price=136.75, config=self.config)
        self.assertLessEqual(result["trigger_price"], 136.75)

    def test_update_required_false_when_change_is_immaterial(self):
        candles = make_base_candles()
        first = compute_deterministic_stop_eod(candles=candles, side="LONG", tick_size=0.05, entry_price=120.0, previous_trigger_price=None, config=self.config)
        second = compute_deterministic_stop_eod(candles=candles, side="LONG", tick_size=0.05, entry_price=120.0, previous_trigger_price=first["trigger_price"], config=self.config)
        self.assertFalse(second["update_required"])

if __name__ == "__main__":
    unittest.main()
