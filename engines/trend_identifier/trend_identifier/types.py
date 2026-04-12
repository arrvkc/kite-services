"""Typed data structures for Trend Identifier."""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import pandas as pd


@dataclass
class ErrorInfo:
    code: str
    message: str


@dataclass
class TimeframeData:
    bars: pd.DataFrame
    adjusted_bars: pd.DataFrame
    missing_bars_adjusted: bool = False


@dataclass
class AlignedData:
    weekly: TimeframeData
    daily: TimeframeData
    hourly: TimeframeData
    instrument: str
    instrument_type: str
    calendar: str
    asof_time: pd.Timestamp
    roll_flag: bool = False
    data_quality_warning: bool = False
    front_contract_volume_series: Optional[pd.Series] = None
    front_contract_oi_series: Optional[pd.Series] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class GateResult:
    passed: bool
    reason_codes: List[str] = field(default_factory=list)
    errors: List[ErrorInfo] = field(default_factory=list)


@dataclass
class TimeframeFeatures:
    nr: Optional[float] = None
    bs: Optional[float] = None
    sep: Optional[float] = None
    bod_up: Optional[float] = None
    bod_dn: Optional[float] = None
    sw: Optional[int] = None
    breakout_sign: Optional[int] = None
    eff: Optional[float] = None
    acc: Optional[float] = None
    ovl: Optional[float] = None
    gap: Optional[float] = None
    volx: Optional[float] = None
    anom: Optional[int] = None
    liq: Optional[int] = None
    atr14: Optional[float] = None
    acceptance_partial: bool = False
    pivot_tie: bool = False
    realized_vol_short: Optional[float] = None
    realized_vol_long: Optional[float] = None


@dataclass
class FeatureBundle:
    weekly: TimeframeFeatures
    daily: TimeframeFeatures
    hourly: TimeframeFeatures
    reason_codes: List[str] = field(default_factory=list)
    diagnostics: Dict[str, Any] = field(default_factory=dict)


@dataclass
class TimeframeScore:
    label: str
    score: Optional[float]
    direction: Optional[float]
    quality: Optional[float]
    noise: Optional[float]


@dataclass
class ScoreBundle:
    weekly: TimeframeScore
    daily: TimeframeScore
    hourly: TimeframeScore
    aggregate_score: Optional[float]


@dataclass
class VetoFlags:
    shock: bool = False
    volatility: bool = False
    anomaly: bool = False
    liquidity: bool = False
    roll: bool = False


@dataclass
class ConflictFlags:
    major: bool = False
    score: bool = False
    quality: bool = False


@dataclass
class DiagnosticsBundle:
    acceptance_partial: bool
    missing_bars_adjusted: bool
    pivot_tie: bool
    hourly_deterioration: bool
    conflicts: Dict[str, bool]
    reason_codes: List[str]


@dataclass
class PreviousState:
    label: str = "FLAT"
    up_candidate_count_consecutive: int = 0
    down_candidate_count_consecutive: int = 0
    hard_up_reversal_count: int = 0
    hard_down_reversal_count: int = 0
    version: int = 0


@dataclass
class FinalPayload:
    spec_version: str
    instrument: str
    asof_time: str
    label: str
    confidence: float
    regime_strength: float
    internal_state: str
    aggregate_score: Optional[float]
    transition_state: str
    roll_flag: bool
    data_quality_warning: bool
    vetoes: Dict[str, bool]
    timeframes: Dict[str, Dict[str, Optional[float]]]
    diagnostics: Dict[str, Any]
    errors: List[Dict[str, str]]
