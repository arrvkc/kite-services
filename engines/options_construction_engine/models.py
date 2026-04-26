from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional

@dataclass(frozen=True)
class OptionsConstructionConfig:
    # Section 10
    min_open_interest: int = 500
    min_volume: int = 100
    max_bid_ask_spread_pct: float = 0.15

    # Pricing/liquidity mode
    # LIVE_STRICT = locked-spec live bid/ask mode
    # AFTER_HOURS_HISTORICAL = non-production test mode using Kite historical last volume-positive candle close
    liquidity_mode: str = "LIVE_STRICT"

    # Section 5
    live_freshness_seconds: int = 300
    freshness_policy: str = "LIVE_5_MINUTES"

    # Section 6/9
    weekly_expiries_enabled: bool = False
    max_expansion_outward_steps: int = 5
    max_expansion_inward_steps: int = 3

    # Section 18
    implementation_build_hash: str = "local-dev"
    audit_log_path: Optional[str] = None

    # Section 19
    exchange: str = "NFO"
    product: str = "NRML"
    order_type: str = "LIMIT"
    validity: str = "DAY"
    variety: str = "regular"

    # Explicit compatibility switch for the internally inconsistent PDF reference fixture.
    # Strict production default remains False.
    reference_fixture_compatibility: bool = False

@dataclass(frozen=True)
class OptionContract:
    tradingsymbol: str
    instrument_token: int | str
    expiry: str
    strike: float
    option_type: str
    bid_price: float
    ask_price: float
    last_price: float
    open_interest: int
    volume: int
    delta: Optional[float]
    data_timestamp: str
    underlying: Optional[str] = None
    lot_size: Optional[int] = None
    delta_source: Optional[str] = None
    delta_timestamp: Optional[str] = None
    delta_source_verified: Optional[bool] = None

@dataclass(frozen=True)
class CandidateLeg:
    role: str
    side: str
    contract: OptionContract

@dataclass(frozen=True)
class Candidate:
    candidate_id: str
    strategy_family: str
    expiry: str
    legs: tuple[CandidateLeg, ...]
    target_deviation: float
    avg_bid_ask_spread_pct: float
    combined_open_interest: int
    combined_volume: int
    ordered_tradingsymbol_tuple: tuple[str, ...]
    target_fit_values: dict[str, Any] = field(default_factory=dict)

@dataclass(frozen=True)
class CandidateEconomics:
    net_premium: float
    width_value: float
    max_loss: float
    max_profit: float
    max_loss_per_lot: float
    max_profit_per_lot: float
    reward_risk_ratio: Optional[float]

@dataclass(frozen=True)
class ScoredCandidate:
    candidate: Candidate
    economics: CandidateEconomics
    liquidity_score: float
    strike_fit_score: float
    reward_risk_score: float
    bid_ask_quality_score: float
    expiry_fit_score: float
    construction_score: int

def strategy_payload_from_result(strategy_result: dict[str, Any]) -> dict[str, Any]:
    """Accept either Strategy Engine wrapper or public payload."""
    if isinstance(strategy_result, dict) and isinstance(strategy_result.get("payload"), dict):
        return strategy_result["payload"]
    return strategy_result
