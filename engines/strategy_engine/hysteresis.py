"""Strategy hysteresis and transition state machine."""
from __future__ import annotations

from .models import HysteresisResult, PreviousStrategyState


LIVE_FAMILIES = {
    "BULL_CALL_SPREAD",
    "BEAR_PUT_SPREAD",
    "BULL_PUT_SPREAD",
    "BEAR_CALL_SPREAD",
    "IRON_CONDOR",
}
DIRECTIONAL = {"BULL_CALL_SPREAD", "BEAR_PUT_SPREAD"}
MILD_OR_NEUTRAL = {"IRON_CONDOR", "BULL_PUT_SPREAD", "BEAR_CALL_SPREAD"}
BULLISH = {"BULL_CALL_SPREAD", "BULL_PUT_SPREAD"}
BEARISH = {"BEAR_PUT_SPREAD", "BEAR_CALL_SPREAD"}


def apply_strategy_hysteresis(
    candidate_family: str,
    candidate_month: str,
    previous_strategy_state: PreviousStrategyState | None,
    final_strategy_strength: int,
) -> HysteresisResult:
    """Apply HS-A through HS-F and the contract-month switching rule."""
    previous = previous_strategy_state or PreviousStrategyState()
    reasons: list[str] = []

    def result(
        family: str,
        month: str,
        transition_state: str,
        pending_family: str | None = None,
        pending_month: str | None = None,
        pending_counter: int = 0,
        extra_reason: str | None = None,
    ) -> HysteresisResult:
        local_reasons = reasons.copy()
        if extra_reason and extra_reason not in local_reasons:
            local_reasons.append(extra_reason)
        return HysteresisResult(
            strategy_family=family,
            contract_month_selection=month,
            strategy_transition_state=transition_state,
            pending_candidate_family=pending_family,
            pending_candidate_month=pending_month,
            pending_counter=pending_counter,
            reason_codes=local_reasons,
        )

    if previous.previous_strategy_family is None:
        if candidate_family != "NO_TRADE" and final_strategy_strength >= 50:
            return result(candidate_family, candidate_month, "fast_track", extra_reason="TRANSITION_FAST_TRACK")
        return result(candidate_family, candidate_month, "stable_initial", extra_reason="TRANSITION_INITIAL")

    prev_family = previous.previous_strategy_family
    prev_month = previous.previous_contract_month_selection

    if candidate_family == prev_family:
        if (
            candidate_family != "NO_TRADE"
            and candidate_month != prev_month
            and prev_month is not None
        ):
            if final_strategy_strength >= 60:
                return result(candidate_family, candidate_month, "stable", extra_reason="TRANSITION_STABLE")
            return result(
                prev_family,
                prev_month,
                "pending_switch",
                pending_family=candidate_family,
                pending_month=candidate_month,
                pending_counter=1,
                extra_reason="TRANSITION_PENDING_SWITCH",
            )
        return result(candidate_family, candidate_month, "stable", extra_reason="TRANSITION_STABLE")

    if prev_family == "NO_TRADE" and candidate_family != "NO_TRADE":
        if final_strategy_strength >= 80:
            return result(candidate_family, candidate_month, "fast_track", extra_reason="TRANSITION_FAST_TRACK")
        if previous.pending_candidate_family == candidate_family and previous.pending_candidate_month == candidate_month:
            return result(candidate_family, candidate_month, "stable", extra_reason="TRANSITION_STABLE")
        return result(
            "NO_TRADE",
            "NO_CONTRACT_MONTH",
            "pending_activation",
            pending_family=candidate_family,
            pending_month=candidate_month,
            pending_counter=1,
            extra_reason="TRANSITION_PENDING_ACTIVATION",
        )

    if prev_family in MILD_OR_NEUTRAL and candidate_family in DIRECTIONAL:
        if final_strategy_strength >= 80:
            return result(candidate_family, candidate_month, "fast_track", extra_reason="TRANSITION_FAST_TRACK")
        if previous.pending_candidate_family == candidate_family and previous.pending_candidate_month == candidate_month:
            return result(candidate_family, candidate_month, "stable", extra_reason="TRANSITION_STABLE")
        return result(
            prev_family,
            prev_month or candidate_month,
            "pending_upgrade",
            pending_family=candidate_family,
            pending_month=candidate_month,
            pending_counter=1,
            extra_reason="TRANSITION_PENDING_UPGRADE",
        )

    opposite_switch = ((prev_family in BULLISH and candidate_family in BEARISH) or (prev_family in BEARISH and candidate_family in BULLISH))
    if opposite_switch:
        if (
            previous.pending_candidate_family == candidate_family
            and previous.pending_candidate_month == candidate_month
            and final_strategy_strength >= 80
        ):
            return result(candidate_family, candidate_month, "fast_track", extra_reason="TRANSITION_FAST_TRACK")
        return result(
            "NO_TRADE",
            "NO_CONTRACT_MONTH",
            "forced_no_trade",
            pending_family=candidate_family,
            pending_month=candidate_month,
            pending_counter=1,
            extra_reason="TRANSITION_FORCED_NO_TRADE",
        )

    if prev_family != "NO_TRADE" and candidate_family == "NO_TRADE":
        return result("NO_TRADE", "NO_CONTRACT_MONTH", "forced_no_trade", extra_reason="TRANSITION_FORCED_NO_TRADE")

    return result(candidate_family, candidate_month, "stable", extra_reason="TRANSITION_STABLE")
