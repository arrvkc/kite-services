"""Hysteresis and transition rules."""

from __future__ import annotations

from typing import Tuple

from .constants import CONSTANTS
from .enums import PublicLabel, ReasonCode, TransitionState
from .types import PreviousState, ScoreBundle, VetoFlags


def apply_hysteresis(candidate: str, previous_state: PreviousState, scores: ScoreBundle, vetoes: VetoFlags, asof_time=None) -> Tuple[str, str, PreviousState, list[str]]:
    # SPEC TRACE: Section 8 - exact hysteresis and transition rules
    reason_codes: list[str] = []
    any_veto_flag = vetoes.shock or vetoes.volatility or vetoes.anomaly or vetoes.liquidity or vetoes.roll
    prev = previous_state.label

    if candidate == prev:
        if candidate == PublicLabel.UP.value:
            previous_state.up_candidate_count_consecutive += 1
        elif candidate == PublicLabel.DOWN.value:
            previous_state.down_candidate_count_consecutive += 1
        return candidate, TransitionState.STABLE.value, previous_state, reason_codes

    if (prev == PublicLabel.FLAT.value) and (candidate == PublicLabel.UP.value):
        if ((previous_state.up_candidate_count_consecutive + 1) >= 2) or ((scores.aggregate_score >= 40) and (scores.weekly.score >= 25)):
            state = TransitionState.FAST_TRACK.value if ((scores.aggregate_score >= 40) and (scores.weekly.score >= 25)) else TransitionState.STABLE.value
            previous_state.label = PublicLabel.UP.value
            previous_state.up_candidate_count_consecutive = 0
            previous_state.down_candidate_count_consecutive = 0
            return PublicLabel.UP.value, state, previous_state, reason_codes
        previous_state.up_candidate_count_consecutive += 1
        reason_codes.append(ReasonCode.TRANSITION_PENDING_UPGRADE.value)
        return PublicLabel.FLAT.value, TransitionState.PENDING_UPGRADE.value, previous_state, reason_codes

    if (prev == PublicLabel.FLAT.value) and (candidate == PublicLabel.DOWN.value):
        if ((previous_state.down_candidate_count_consecutive + 1) >= 2) or ((scores.aggregate_score <= -40) and (scores.weekly.score <= -25)):
            state = TransitionState.FAST_TRACK.value if ((scores.aggregate_score <= -40) and (scores.weekly.score <= -25)) else TransitionState.STABLE.value
            previous_state.label = PublicLabel.DOWN.value
            previous_state.down_candidate_count_consecutive = 0
            previous_state.up_candidate_count_consecutive = 0
            return PublicLabel.DOWN.value, state, previous_state, reason_codes
        previous_state.down_candidate_count_consecutive += 1
        reason_codes.append(ReasonCode.TRANSITION_PENDING_DOWNGRADE.value)
        return PublicLabel.FLAT.value, TransitionState.PENDING_DOWNGRADE.value, previous_state, reason_codes

    if (prev == PublicLabel.UP.value) and (candidate == PublicLabel.DOWN.value):
        strong = ((scores.aggregate_score <= -50) and (scores.weekly.score <= -25) and (scores.daily.score <= -25))
        previous_state.hard_down_reversal_count = previous_state.hard_down_reversal_count + 1 if strong else 0
        if previous_state.hard_down_reversal_count >= 2:
            previous_state.label = PublicLabel.DOWN.value
            return PublicLabel.DOWN.value, TransitionState.FAST_TRACK.value, previous_state, reason_codes
        reason_codes.append(ReasonCode.TRANSITION_FORCED_FLAT.value)
        previous_state.label = PublicLabel.FLAT.value
        return PublicLabel.FLAT.value, TransitionState.FORCED_FLAT.value, previous_state, reason_codes

    if (prev == PublicLabel.DOWN.value) and (candidate == PublicLabel.UP.value):
        strong = ((scores.aggregate_score >= 50) and (scores.weekly.score >= 25) and (scores.daily.score >= 25))
        previous_state.hard_up_reversal_count = previous_state.hard_up_reversal_count + 1 if strong else 0
        if previous_state.hard_up_reversal_count >= 2:
            previous_state.label = PublicLabel.UP.value
            return PublicLabel.UP.value, TransitionState.FAST_TRACK.value, previous_state, reason_codes
        reason_codes.append(ReasonCode.TRANSITION_FORCED_FLAT.value)
        previous_state.label = PublicLabel.FLAT.value
        return PublicLabel.FLAT.value, TransitionState.FORCED_FLAT.value, previous_state, reason_codes

    if ((prev in [PublicLabel.UP.value, PublicLabel.DOWN.value]) and (candidate == PublicLabel.FLAT.value)):
        if (abs(scores.aggregate_score) < 15) or any_veto_flag:
            previous_state.label = PublicLabel.FLAT.value
            reason_codes.append(ReasonCode.TRANSITION_FORCED_FLAT.value)
            return PublicLabel.FLAT.value, TransitionState.FORCED_FLAT.value, previous_state, reason_codes
        # SPEC NOTE: document says require one confirming daily evaluation; implementation keeps prior label with pending downgrade
        if prev == PublicLabel.UP.value:
            previous_state.down_candidate_count_consecutive += 1
        else:
            previous_state.up_candidate_count_consecutive += 1
        reason_codes.append(ReasonCode.TRANSITION_PENDING_DOWNGRADE.value)
        return prev, TransitionState.PENDING_DOWNGRADE.value, previous_state, reason_codes

    previous_state.label = candidate
    return candidate, TransitionState.STABLE.value, previous_state, reason_codes
