"""Typed models for the Strategy Engine."""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional


@dataclass(frozen=True)
class ErrorObject:
    code: str
    message: str


@dataclass(frozen=True)
class TrendPayloadSnapshot:
    instrument: str
    asof_time: str
    label: str
    confidence: float
    aggregate_score: float | None
    internal_state: str


@dataclass(frozen=True)
class W5HistoryRow:
    label: str
    confidence: float
    aggregate_score: float


@dataclass(frozen=True)
class StrategyInput:
    instrument: str
    latest_payload: TrendPayloadSnapshot
    trend_history_w5: List[W5HistoryRow]
    dte_near_month: int
    next_month_available: bool
    dte_next_month: int | None
    in_universe: bool = True
    duplicate_payload: bool = False
    is_completed_daily_run: bool = True


@dataclass(frozen=True)
class ValidatedInputBundle:
    instrument: str
    latest_payload: TrendPayloadSnapshot
    trend_history_w5: List[W5HistoryRow]
    dte_near_month: int
    next_month_available: bool
    dte_next_month: int | None
    in_universe: bool
    duplicate_payload: bool
    is_completed_daily_run: bool


@dataclass(frozen=True)
class GateResult:
    passed: bool
    reason_codes: List[str]
    errors: List[ErrorObject] = field(default_factory=list)


@dataclass(frozen=True)
class HistoryMetrics:
    s_t: float
    c_t: float
    l_t: str
    bull_count_5: int
    bear_count_5: int
    flat_count_5: int
    mean_abs_score_5: float
    mean_score_3: float
    mean_conf_3: float
    sign_flip_count_5: int


@dataclass(frozen=True)
class CandidateSelectionResult:
    strategy_family: str
    reason_codes: List[str]


@dataclass(frozen=True)
class ContractMonthSelectionResult:
    strategy_family: str
    contract_month_selection: str
    reason_codes: List[str]


@dataclass(frozen=True)
class PreviousStrategyState:
    previous_strategy_family: str | None = None
    previous_contract_month_selection: str | None = None
    pending_candidate_family: str | None = None
    pending_candidate_month: str | None = None
    pending_counter: int = 0


@dataclass(frozen=True)
class HysteresisResult:
    strategy_family: str
    contract_month_selection: str
    strategy_transition_state: str
    pending_candidate_family: str | None
    pending_candidate_month: str | None
    pending_counter: int
    reason_codes: List[str]


@dataclass
class StrategyOutputPayload:
    spec_version: str
    instrument: str
    asof_time: str
    strategy_family: str
    contract_month_selection: str
    final_strategy_strength: int
    include_in_top_n: bool
    rank_overall: int | None
    rank_in_family: int | None
    strategy_transition_state: str
    reason_codes: List[str]
    input_snapshot: Dict[str, Any]
    errors: List[Dict[str, str]]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
