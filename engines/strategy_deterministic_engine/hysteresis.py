from __future__ import annotations

from typing import Any

from .constants import (
    FAST_TRACK_STRENGTH,
    FAMILY_BEAR_CALL_SPREAD,
    FAMILY_BEAR_PUT_SPREAD,
    FAMILY_BULL_CALL_SPREAD,
    FAMILY_BULL_PUT_SPREAD,
    FAMILY_IRON_CONDOR,
    REASON_TRANSITION_FAST_TRACK,
    REASON_TRANSITION_FORCED_INTERMEDIATE,
    REASON_TRANSITION_INITIAL,
    REASON_TRANSITION_PENDING_ACTIVATION,
    REASON_TRANSITION_PENDING_SWITCH,
    REASON_TRANSITION_PENDING_UPGRADE,
    REASON_TRANSITION_STABLE,
    TRANSITION_FAST_TRACK,
    TRANSITION_FORCED_INTERMEDIATE,
    TRANSITION_PENDING_ACTIVATION,
    TRANSITION_PENDING_SWITCH,
    TRANSITION_PENDING_UPGRADE,
    TRANSITION_STABLE,
    TRANSITION_STABLE_INITIAL,
)

def _normalize_previous_state(previous_state: dict[str, Any] | None) -> dict[str, Any] | None:
    if not previous_state:
        return None
    return {
        "strategy_family": previous_state.get("strategy_family"),
        "contract_month_selection": previous_state.get("contract_month_selection"),
        "strategy_transition_state": previous_state.get("strategy_transition_state"),
        "pending_counter": int(previous_state.get("pending_counter", 0) or 0),
        "pending_candidate_family": previous_state.get("pending_candidate_family"),
        "pending_candidate_month": previous_state.get("pending_candidate_month"),
    }

def _base_next_state(final_family: str, final_month: str, transition_state: str, pending_counter: int = 0, pending_candidate_family: str | None = None, pending_candidate_month: str | None = None) -> dict[str, Any]:
    return {
        "strategy_family": final_family,
        "contract_month_selection": final_month,
        "strategy_transition_state": transition_state,
        "pending_counter": pending_counter,
        "pending_candidate_family": pending_candidate_family,
        "pending_candidate_month": pending_candidate_month,
    }

def apply_strategy_hysteresis(candidate_family: str, candidate_month: str, previous_state: dict[str, Any] | None, final_strategy_strength: int, aggregate_score: float) -> tuple[str, str, str, list[str], dict[str, Any]]:
    previous = _normalize_previous_state(previous_state)
    if previous is None:
        if final_strategy_strength >= 50:
            transition_state = TRANSITION_FAST_TRACK
            reasons = [REASON_TRANSITION_FAST_TRACK]
        else:
            transition_state = TRANSITION_STABLE_INITIAL
            reasons = [REASON_TRANSITION_INITIAL]
        next_state = _base_next_state(candidate_family, candidate_month, transition_state)
        return candidate_family, candidate_month, transition_state, reasons, next_state
    previous_family = previous["strategy_family"]
    previous_month = previous["contract_month_selection"]
    pending_counter = previous["pending_counter"]
    pending_family = previous["pending_candidate_family"]
    pending_month = previous["pending_candidate_month"]

    if candidate_family == previous_family and candidate_month == previous_month:
        next_state = _base_next_state(candidate_family, candidate_month, TRANSITION_STABLE)
        return candidate_family, candidate_month, TRANSITION_STABLE, [REASON_TRANSITION_STABLE], next_state

    if candidate_family == previous_family and candidate_month != previous_month:
        if final_strategy_strength >= 60:
            next_state = _base_next_state(candidate_family, candidate_month, TRANSITION_STABLE)
            return candidate_family, candidate_month, TRANSITION_STABLE, [REASON_TRANSITION_STABLE], next_state
        if pending_family == candidate_family and pending_month == candidate_month and pending_counter >= 1:
            next_state = _base_next_state(candidate_family, candidate_month, TRANSITION_STABLE)
            return candidate_family, candidate_month, TRANSITION_STABLE, [REASON_TRANSITION_STABLE], next_state
        next_state = _base_next_state(
            previous_family,
            previous_month,
            TRANSITION_PENDING_SWITCH,
            pending_counter=1,
            pending_candidate_family=candidate_family,
            pending_candidate_month=candidate_month,
        )
        return previous_family, previous_month, TRANSITION_PENDING_SWITCH, [REASON_TRANSITION_PENDING_SWITCH], next_state

    if previous_family == FAMILY_IRON_CONDOR and candidate_family in {FAMILY_BULL_PUT_SPREAD, FAMILY_BEAR_CALL_SPREAD}:
        if final_strategy_strength >= FAST_TRACK_STRENGTH:
            next_state = _base_next_state(candidate_family, candidate_month, TRANSITION_FAST_TRACK)
            return candidate_family, candidate_month, TRANSITION_FAST_TRACK, [REASON_TRANSITION_FAST_TRACK], next_state
        if pending_family == candidate_family and pending_month == candidate_month and pending_counter >= 1:
            next_state = _base_next_state(candidate_family, candidate_month, TRANSITION_STABLE)
            return candidate_family, candidate_month, TRANSITION_STABLE, [REASON_TRANSITION_STABLE], next_state
        next_state = _base_next_state(
            previous_family,
            previous_month,
            TRANSITION_PENDING_ACTIVATION,
            pending_counter=1,
            pending_candidate_family=candidate_family,
            pending_candidate_month=candidate_month,
        )
        return previous_family, previous_month, TRANSITION_PENDING_ACTIVATION, [REASON_TRANSITION_PENDING_ACTIVATION], next_state

    if previous_family in {FAMILY_IRON_CONDOR, FAMILY_BULL_PUT_SPREAD, FAMILY_BEAR_CALL_SPREAD} and candidate_family in {FAMILY_BULL_CALL_SPREAD, FAMILY_BEAR_PUT_SPREAD}:
        if final_strategy_strength >= FAST_TRACK_STRENGTH:
            next_state = _base_next_state(candidate_family, candidate_month, TRANSITION_FAST_TRACK)
            return candidate_family, candidate_month, TRANSITION_FAST_TRACK, [REASON_TRANSITION_FAST_TRACK], next_state
        if pending_family == candidate_family and pending_month == candidate_month and pending_counter >= 1:
            next_state = _base_next_state(candidate_family, candidate_month, TRANSITION_STABLE)
            return candidate_family, candidate_month, TRANSITION_STABLE, [REASON_TRANSITION_STABLE], next_state
        next_state = _base_next_state(
            previous_family,
            previous_month,
            TRANSITION_PENDING_UPGRADE,
            pending_counter=1,
            pending_candidate_family=candidate_family,
            pending_candidate_month=candidate_month,
        )
        return previous_family, previous_month, TRANSITION_PENDING_UPGRADE, [REASON_TRANSITION_PENDING_UPGRADE], next_state

    bullish_families = {FAMILY_BULL_CALL_SPREAD, FAMILY_BULL_PUT_SPREAD}
    bearish_families = {FAMILY_BEAR_PUT_SPREAD, FAMILY_BEAR_CALL_SPREAD}
    opposite_switch = (previous_family in bullish_families and candidate_family in bearish_families) or (previous_family in bearish_families and candidate_family in bullish_families)
    if opposite_switch:
        if pending_family == candidate_family and pending_month == candidate_month and pending_counter >= 1 and final_strategy_strength >= FAST_TRACK_STRENGTH:
            next_state = _base_next_state(candidate_family, candidate_month, TRANSITION_FAST_TRACK)
            return candidate_family, candidate_month, TRANSITION_FAST_TRACK, [REASON_TRANSITION_FAST_TRACK], next_state
        next_state = _base_next_state(
            FAMILY_IRON_CONDOR,
            candidate_month,
            TRANSITION_FORCED_INTERMEDIATE,
            pending_counter=1,
            pending_candidate_family=candidate_family,
            pending_candidate_month=candidate_month,
        )
        return FAMILY_IRON_CONDOR, candidate_month, TRANSITION_FORCED_INTERMEDIATE, [REASON_TRANSITION_FORCED_INTERMEDIATE], next_state

    if candidate_family == FAMILY_IRON_CONDOR and previous_family in {FAMILY_BULL_CALL_SPREAD, FAMILY_BULL_PUT_SPREAD, FAMILY_BEAR_PUT_SPREAD, FAMILY_BEAR_CALL_SPREAD}:
        if abs(aggregate_score) < 15:
            next_state = _base_next_state(candidate_family, candidate_month, TRANSITION_STABLE)
            return candidate_family, candidate_month, TRANSITION_STABLE, [REASON_TRANSITION_STABLE], next_state
        if pending_family == candidate_family and pending_month == candidate_month and pending_counter >= 1:
            next_state = _base_next_state(candidate_family, candidate_month, TRANSITION_STABLE)
            return candidate_family, candidate_month, TRANSITION_STABLE, [REASON_TRANSITION_STABLE], next_state
        next_state = _base_next_state(
            previous_family,
            previous_month,
            TRANSITION_PENDING_ACTIVATION,
            pending_counter=1,
            pending_candidate_family=candidate_family,
            pending_candidate_month=candidate_month,
        )
        return previous_family, previous_month, TRANSITION_PENDING_ACTIVATION, [REASON_TRANSITION_PENDING_ACTIVATION], next_state

    next_state = _base_next_state(candidate_family, candidate_month, TRANSITION_STABLE)
    return candidate_family, candidate_month, TRANSITION_STABLE, [REASON_TRANSITION_STABLE], next_state
