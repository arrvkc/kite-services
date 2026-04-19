"""Contract month selection logic."""
from __future__ import annotations

from . import constants as c
from .models import ContractMonthSelectionResult


DEBIT_FAMILIES = {"BULL_CALL_SPREAD", "BEAR_PUT_SPREAD"}
CREDIT_FAMILIES = {"BULL_PUT_SPREAD", "BEAR_CALL_SPREAD"}


def select_contract_month(
    candidate_family: str,
    dte_near_month: int,
    dte_next_month: int | None,
    next_month_available: bool,
) -> ContractMonthSelectionResult:
    """Apply CM-0 through CM-I in locked order."""
    reasons: list[str] = []

    if (
        dte_near_month > 1
        and dte_near_month <= c.FORCE_NEXT_BUFFER
        and next_month_available is True
        and dte_next_month is not None
        and dte_next_month >= c.NEXT_MONTH_MIN_DTE
    ):
        return ContractMonthSelectionResult(candidate_family, "NEXT_MONTH", ["CONTRACT_NEXT_MONTH_DTE"])

    if candidate_family == "NO_TRADE":
        return ContractMonthSelectionResult("NO_TRADE", "NO_CONTRACT_MONTH", reasons)

    if dte_near_month <= c.FORCE_NO_TRADE_CLOSEOUT:
        return ContractMonthSelectionResult("NO_TRADE", "NO_CONTRACT_MONTH", reasons)

    if candidate_family in DEBIT_FAMILIES and dte_near_month >= c.DEBIT_MIN_NEAR_DTE:
        return ContractMonthSelectionResult(candidate_family, "NEAR_MONTH", ["CONTRACT_NEAR_MONTH_DTE"])

    if (
        candidate_family in DEBIT_FAMILIES
        and c.MIN_DTE_ANY <= dte_near_month < c.DEBIT_MIN_NEAR_DTE
        and next_month_available is True
        and dte_next_month is not None
        and dte_next_month >= c.NEXT_MONTH_MIN_DTE
    ):
        return ContractMonthSelectionResult(candidate_family, "NEXT_MONTH", ["CONTRACT_NEXT_MONTH_DTE"])

    if candidate_family in CREDIT_FAMILIES and dte_near_month >= c.CREDIT_MIN_NEAR_DTE:
        return ContractMonthSelectionResult(candidate_family, "NEAR_MONTH", ["CONTRACT_NEAR_MONTH_DTE"])

    if (
        candidate_family in CREDIT_FAMILIES
        and c.MIN_DTE_ANY <= dte_near_month < c.CREDIT_MIN_NEAR_DTE
        and next_month_available is True
        and dte_next_month is not None
        and dte_next_month >= c.NEXT_MONTH_MIN_DTE
    ):
        return ContractMonthSelectionResult(candidate_family, "NEXT_MONTH", ["CONTRACT_NEXT_MONTH_DTE"])

    if candidate_family == "IRON_CONDOR" and dte_near_month >= c.CONDOR_MIN_NEAR_DTE:
        return ContractMonthSelectionResult(candidate_family, "NEAR_MONTH", ["CONTRACT_NEAR_MONTH_DTE"])

    if (
        candidate_family == "IRON_CONDOR"
        and c.MIN_DTE_ANY <= dte_near_month < c.CONDOR_MIN_NEAR_DTE
        and next_month_available is True
        and dte_next_month is not None
        and dte_next_month >= c.NEXT_MONTH_MIN_DTE
    ):
        return ContractMonthSelectionResult(candidate_family, "NEXT_MONTH", ["CONTRACT_NEXT_MONTH_DTE"])

    if c.MIN_DTE_ANY <= dte_near_month and not next_month_available:
        reasons.append("GATE_NEXT_MONTH_UNAVAILABLE")
    return ContractMonthSelectionResult("NO_TRADE", "NO_CONTRACT_MONTH", reasons)
