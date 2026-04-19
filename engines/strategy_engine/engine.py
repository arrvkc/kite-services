"""Engine orchestration."""
from __future__ import annotations

import json
import logging
from typing import Iterable, List, Sequence

from .contract_month import select_contract_month
from .family_selection import select_candidate_family
from .hysteresis import apply_strategy_hysteresis
from .metrics import compute_history_metrics
from .models import PreviousStrategyState, StrategyInput, StrategyOutputPayload
from .output import public_output
from .ranking import rank_candidates
from .strength import apply_contract_month_adjustment, compute_base_strategy_strength
from .validators import ValidationError, passes_strategy_gate, validate_inputs

logger = logging.getLogger(__name__)


def evaluate_strategy_engine(
    strategy_input: StrategyInput,
    previous_strategy_state: PreviousStrategyState | None = None,
) -> tuple[StrategyOutputPayload, PreviousStrategyState]:
    """Evaluate one instrument deterministically."""
    logger.info("event=input_validation instrument=%s", strategy_input.instrument)
    bundle = validate_inputs(strategy_input)

    gate = passes_strategy_gate(bundle)
    logger.info(
        "event=gate_result instrument=%s passed=%s reason_codes=%s",
        strategy_input.instrument,
        gate.passed,
        json.dumps(gate.reason_codes),
    )
    if not gate.passed:
        payload = public_output(
            bundle=bundle,
            metrics=None,
            strategy_family="NO_TRADE",
            contract_month_selection="NO_CONTRACT_MONTH",
            final_strategy_strength=0,
            transition_state="forced_no_trade" if previous_strategy_state and previous_strategy_state.previous_strategy_family not in {None, "NO_TRADE"} else "stable_initial",
            reason_codes=gate.reason_codes,
            errors=gate.errors,
        )
        next_state = PreviousStrategyState(
            previous_strategy_family=payload.strategy_family,
            previous_contract_month_selection=payload.contract_month_selection,
            pending_candidate_family=None,
            pending_candidate_month=None,
            pending_counter=0,
        )
        return payload, next_state

    metrics = compute_history_metrics(bundle)
    candidate = select_candidate_family(metrics)
    logger.info("event=candidate_selected instrument=%s family=%s", strategy_input.instrument, candidate.strategy_family)

    contract = select_contract_month(
        candidate.strategy_family,
        bundle.dte_near_month,
        bundle.dte_next_month,
        bundle.next_month_available,
    )
    logger.info(
        "event=contract_month_selected instrument=%s family=%s month=%s",
        strategy_input.instrument,
        contract.strategy_family,
        contract.contract_month_selection,
    )

    if contract.strategy_family == "NO_TRADE":
        final_strength = 0
    else:
        base_strength = compute_base_strategy_strength(contract.strategy_family, metrics)
        final_strength = apply_contract_month_adjustment(base_strength, contract.strategy_family, contract.contract_month_selection)
    logger.info("event=strength_computed instrument=%s strength=%s", strategy_input.instrument, final_strength)

    hysteresis = apply_strategy_hysteresis(
        candidate_family=contract.strategy_family,
        candidate_month=contract.contract_month_selection,
        previous_strategy_state=previous_strategy_state,
        final_strategy_strength=final_strength,
    )
    logger.info(
        "event=hysteresis_result instrument=%s family=%s month=%s state=%s",
        strategy_input.instrument,
        hysteresis.strategy_family,
        hysteresis.contract_month_selection,
        hysteresis.strategy_transition_state,
    )

    payload = public_output(
        bundle=bundle,
        metrics=metrics,
        strategy_family=hysteresis.strategy_family,
        contract_month_selection=hysteresis.contract_month_selection,
        final_strategy_strength=final_strength,
        transition_state=hysteresis.strategy_transition_state,
        reason_codes=candidate.reason_codes + contract.reason_codes + hysteresis.reason_codes,
        errors=[],
    )
    logger.info("event=payload_emitted instrument=%s", strategy_input.instrument)

    next_state = PreviousStrategyState(
        previous_strategy_family=payload.strategy_family,
        previous_contract_month_selection=payload.contract_month_selection,
        pending_candidate_family=hysteresis.pending_candidate_family,
        pending_candidate_month=hysteresis.pending_candidate_month,
        pending_counter=hysteresis.pending_counter,
    )
    return payload, next_state


def evaluate_batch(
    strategy_inputs: Sequence[StrategyInput],
    previous_states: dict[str, PreviousStrategyState] | None = None,
) -> tuple[List[StrategyOutputPayload], dict[str, PreviousStrategyState]]:
    """Evaluate a batch and then rank outputs."""
    previous_states = previous_states or {}
    outputs: List[StrategyOutputPayload] = []
    next_states: dict[str, PreviousStrategyState] = {}
    for strategy_input in strategy_inputs:
        payload, next_state = evaluate_strategy_engine(strategy_input, previous_states.get(strategy_input.instrument))
        outputs.append(payload)
        next_states[strategy_input.instrument] = next_state
    ranked = rank_candidates(outputs)
    logger.info("event=ranking_completed batch_size=%s", len(ranked))
    return ranked, next_states
