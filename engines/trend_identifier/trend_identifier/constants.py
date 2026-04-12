"""Locked constants from the v2.4 specification."""

from dataclasses import dataclass, field
from typing import Dict, List


@dataclass(frozen=True)
class TimeframeConstants:
    direction_k_nr: float
    direction_k_bs: float
    direction_k_sep: float
    eff_min: float
    acc_min: float
    up_threshold: float
    down_threshold: float
    quality_min: float
    gap_veto: float
    volx_veto: float
    weight: float


@dataclass(frozen=True)
class Constants:
    spec_version: str = "v2.4"
    weekly_bars_required: int = 104
    daily_bars_required: int = 252
    hourly_bars_required: int = 480
    daily_missing_tolerance: float = 0.03
    hourly_missing_tolerance: float = 0.05
    daily_missing_window: int = 60
    hourly_missing_window: int = 240
    equity_liquidity_threshold_usd: float = 5_000_000.0
    weekly_lookbacks: Dict[str, int] = field(default_factory=lambda: {
        "ret": 13, "slope": 4, "fast": 13, "slow": 26, "break": 26, "eff": 13,
        "acc": 6, "ovl": 8, "rv_short": 4, "rv_long": 26, "pivot_left": 2, "pivot_right": 2
    })
    daily_lookbacks: Dict[str, int] = field(default_factory=lambda: {
        "ret": 60, "slope": 10, "fast": 20, "slow": 60, "break": 60, "eff": 40,
        "acc": 15, "ovl": 20, "rv_short": 10, "rv_long": 60, "pivot_left": 3, "pivot_right": 3
    })
    hourly_lookbacks: Dict[str, int] = field(default_factory=lambda: {
        "ret": 48, "slope": 12, "fast": 24, "slow": 120, "break": 48, "eff": 36,
        "acc": 12, "ovl": 24, "rv_short": 10, "rv_long": 60, "pivot_left": 2, "pivot_right": 2
    })
    session_bars_per_day_cash: int = 6
    shock_gap_daily: float = 2.0
    shock_gap_weekly: float = 2.0
    shock_acc_daily: float = 0.60
    volatility_veto_daily: float = 1.75
    volatility_veto_eff_daily: float = 0.30
    anomaly_true_range_multiplier: float = 4.0
    anomaly_median_window: int = 20
    roll_hourly_weight_cap: float = 0.05
    roll_gap_suppression_atr: float = 1.5
    roll_oi_ratio: float = 0.80
    confidence_aggregate_divisor: float = 50.0
    regime_strength_divisor: float = 40.0
    aggregate_up_threshold: float = 25.0
    aggregate_down_threshold: float = -25.0
    aggregate_veto_neutral_band: float = 35.0
    flat_to_up_fast_track_aggregate: float = 40.0
    flat_to_up_fast_track_weekly: float = 25.0
    flat_to_down_fast_track_aggregate: float = -40.0
    flat_to_down_fast_track_weekly: float = -25.0
    direct_reversal_aggregate: float = 50.0
    direct_reversal_score: float = 25.0
    flat_degrade_immediate_abs_aggregate: float = 15.0
    accepted_transitions_need_consecutive: int = 2
    futures_notional_volume_threshold_default: float = 5_000_000.0
    allowed_public_labels: List[str] = field(default_factory=lambda: ["UP", "FLAT", "DOWN"])
    timeframes: Dict[str, TimeframeConstants] = field(default_factory=lambda: {
        "weekly": TimeframeConstants(6.0, 0.12, 1.50, 0.38, 0.67, 22.0, -22.0, 0.15, 2.0, 1.80, 0.50),
        "daily": TimeframeConstants(4.0, 0.10, 1.20, 0.34, 0.60, 18.0, -18.0, 0.10, 2.0, 1.75, 0.35),
        "hourly": TimeframeConstants(3.0, 0.08, 1.00, 0.30, 0.58, 15.0, -15.0, 0.05, 1.8, 1.60, 0.15),
    })


CONSTANTS = Constants()
