from __future__ import annotations

from .constants import (
    CONDOR_PREFERRED_NEAR_DTE,
    CONTRACT_NEAR_MONTH,
    CONTRACT_NEXT_MONTH,
    CREDIT_PREFERRED_NEAR_DTE,
    DEBIT_PREFERRED_NEAR_DTE,
    FAMILY_BEAR_CALL_SPREAD,
    FAMILY_BEAR_PUT_SPREAD,
    FAMILY_BULL_CALL_SPREAD,
    FAMILY_BULL_PUT_SPREAD,
    MIN_DTE_ANY,
    NEXT_MONTH_MIN_DTE,
    REASON_CONTRACT_NEAR_MONTH_DTE,
    REASON_CONTRACT_NEXT_MONTH_DTE,
)

def _preferred_near_threshold(strategy_family: str) -> int:
    if strategy_family in {FAMILY_BULL_CALL_SPREAD, FAMILY_BEAR_PUT_SPREAD}:
        return DEBIT_PREFERRED_NEAR_DTE
    if strategy_family in {FAMILY_BULL_PUT_SPREAD, FAMILY_BEAR_CALL_SPREAD}:
        return CREDIT_PREFERRED_NEAR_DTE
    return CONDOR_PREFERRED_NEAR_DTE

def select_contract_month(candidate_family: str, dte_near_month: int, dte_next_month: int | None, next_month_available: bool) -> tuple[str, list[str]]:
    preferred_near_threshold = _preferred_near_threshold(candidate_family)
    if dte_near_month >= preferred_near_threshold:
        return CONTRACT_NEAR_MONTH, [REASON_CONTRACT_NEAR_MONTH_DTE]
    if next_month_available and dte_next_month is not None and dte_next_month >= NEXT_MONTH_MIN_DTE:
        return CONTRACT_NEXT_MONTH, [REASON_CONTRACT_NEXT_MONTH_DTE]
    if dte_near_month >= MIN_DTE_ANY:
        return CONTRACT_NEAR_MONTH, [REASON_CONTRACT_NEAR_MONTH_DTE]
    if next_month_available:
        return CONTRACT_NEXT_MONTH, [REASON_CONTRACT_NEXT_MONTH_DTE]
    return CONTRACT_NEAR_MONTH, [REASON_CONTRACT_NEAR_MONTH_DTE]
