from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


@dataclass(frozen=True)
class Config:
    scope: str
    clients: List[str]
    segment: str
    side: str
    symbol: Optional[str]
    exchange: Optional[str]
    product: Optional[str]
    mode: str
    confirm: Optional[str]
    cancel_gtt: bool
    parallel_workers: int
    pause_ms: int
    max_orders: Optional[int]
    max_clients: Optional[int]
    max_gtt_cancel: Optional[int]
    retry_attempts: int
    reason: Optional[str]
    operation_tag: Optional[str]
    output_format: str
    output_path: Optional[str]
    verbose: bool
    quiet: bool

    @property
    def is_live(self) -> bool:
        return self.mode == 'execute'


@dataclass(frozen=True)
class PositionRecord:
    client_id: str
    exchange: str
    underlying_symbol: str
    tradingsymbol: str
    product: Optional[str]
    net_quantity: int
    average_price: Optional[float] = None
    pnl: Optional[float] = None
    expiry: Optional[str] = None
    tick_size: Optional[float] = None
    instrument_token: Optional[int] = None

    @property
    def position_side(self) -> str:
        return 'LONG' if self.net_quantity > 0 else 'SHORT'


@dataclass(frozen=True)
class GttRecord:
    client_id: str
    trigger_id: str
    exchange: str
    tradingsymbol: str
    transaction_type: str
    product: Optional[str]
    trigger_values: List[float]
    last_price: Optional[float]
    status: Optional[str]
    raw: Dict[str, Any]


@dataclass(frozen=True)
class PlannedExitOrder:
    client_id: str
    exchange: str
    tradingsymbol: str
    exit_side: str
    quantity: int
    product: Optional[str]
    order_type: str = 'MARKET'


@dataclass
class TaskResult:
    client_id: str
    tradingsymbol: str
    status: str
    message: str
    trigger_id: Optional[str] = None
    exit_side: Optional[str] = None
    quantity: Optional[int] = None
    broker_order_id: Optional[str] = None


@dataclass
class Summary:
    matched_clients_count: int = 0
    matched_positions_count: int = 0
    planned_orders_count: int = 0
    matched_gtts_count: int = 0
    gtt_cancel_success_count: int = 0
    gtt_cancel_failure_count: int = 0
    order_success_count: int = 0
    order_failure_count: int = 0
    skipped_count: int = 0


@dataclass
class OperationRecord:
    operation_id: str
    timestamp_utc: str
    mode: str
    raw_args: List[str]
    filters: Dict[str, Any]
    selected_clients: List[str]
    matched_positions: List[Dict[str, Any]]
    matched_gtts: List[Dict[str, Any]]
    planned_exit_orders: List[Dict[str, Any]]
    gtt_cancel_results: List[Dict[str, Any]]
    order_results: List[Dict[str, Any]]
    summary: Dict[str, Any]
    reason: Optional[str]
    operation_tag: Optional[str]
    exit_code: int

    @staticmethod
    def now_utc() -> str:
        return datetime.now(timezone.utc).isoformat()

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
