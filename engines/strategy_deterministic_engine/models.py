from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

@dataclass(frozen=True)
class HistoryMetrics:
    bull_count_5: int
    bear_count_5: int
    flat_count_5: int
    mean_abs_score_5: float
    mean_score_3: float
    mean_conf_3: float
    sign_flip_count_5: int
    instability_5: float

@dataclass(frozen=True)
class ValidatedBundle:
    instrument: str
    asof_time: str
    label: str
    confidence: float
    aggregate_score: float
    internal_state: str
    trend_history_w5: list[dict[str, Any]]
    dte_near_month: int
    next_month_available: bool
    dte_next_month: int | None
    in_universe: bool
    prior_committed_state: dict[str, Any] | None

@dataclass(frozen=True)
class InvalidEvaluationRecord:
    instrument: str
    asof_time: str | None
    reason_codes: list[str]
    errors: list[dict[str, str]]
    input_snapshot: dict[str, Any] = field(default_factory=dict)

from typing import Optional, List


@dataclass(frozen=True)
class W5HistoryRow:
    label: str
    confidence: float
    aggregate_score: float


@dataclass(frozen=True)
class TrendPayloadSnapshot:
    instrument: str
    asof_time: str
    label: str
    confidence: float
    aggregate_score: Optional[float]
    internal_state: str


@dataclass(frozen=True)
class PriorCommittedState:
    instrument: str
    asof_time: str
    strategy_family: str
    contract_month_selection: str
    strategy_transition_state: str
    pending_counter: int
    pending_candidate_family: Optional[str]
    pending_candidate_month: Optional[str]
    state_version: str


@dataclass(frozen=True)
class StrategyInput:
    instrument: str
    latest_payload: TrendPayloadSnapshot
    trend_history_w5: List[W5HistoryRow]
    dte_near_month: int
    next_month_available: bool
    dte_next_month: Optional[int]
    in_universe: bool
    duplicate_payload: bool
    is_completed_daily_run: bool
    prior_committed_state: Optional[PriorCommittedState] = None
