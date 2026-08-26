"""Stable pure boundary for reviewed five-family option construction.

The caller supplies one coherent current-market observation.  This module has
no broker client, credentials, network call or execution surface; it delegates
unchanged financial rules to :class:`OptionsConstructionEngine`.
"""

from __future__ import annotations

from collections import Counter
from copy import deepcopy
from decimal import Decimal
from typing import Any, Mapping, Sequence

from .engine import OptionsConstructionEngine
from .models import OptionsConstructionConfig


CONTRACT_IDENTITY = "EAJEE_OPTIONS_CONSTRUCTION_SUPPLIED_MARKET_FACTS"


def prepare_supplied_option_market_context(
    option_chain: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Expose the reviewed runner's strike-step and lot-size conventions.

    This is a pure extraction of its existing supplied-chain preparation:
    minimum positive listed-strike interval, dominant positive lot size, and
    filtering to that lot size. No quote is fetched and no threshold changes.
    """
    if not isinstance(option_chain, Sequence):
        raise TypeError("Supplied option chain must use a sequence contract.")
    chain = deepcopy([dict(item) for item in option_chain])
    strikes = sorted({
        Decimal(str(item["strike"]))
        for item in chain
        if item.get("strike") is not None and Decimal(str(item["strike"])) > 0
    })
    if len(strikes) < 2:
        raise ValueError("Not enough listed strikes to infer strike_step.")
    strike_steps = [
        upper - lower
        for lower, upper in zip(strikes, strikes[1:])
        if upper > lower
    ]
    lot_counts = Counter(
        int(item["lot_size"])
        for item in chain
        if item.get("lot_size") is not None and int(item["lot_size"]) > 0
    )
    if not lot_counts:
        raise ValueError("No positive lot_size found in supplied option chain.")
    lot_size = lot_counts.most_common(1)[0][0]
    return {
        "strike_step": min(strike_steps),
        "lot_size": lot_size,
        "option_chain": tuple(
            item for item in chain if int(item.get("lot_size") or 0) == lot_size
        ),
    }


def _engine_value(value: Any) -> Any:
    """Translate exact external decimals at the locked float-based engine edge."""
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("Supplied market Decimal must be finite.")
        return float(str(value))
    if isinstance(value, Mapping):
        return {str(key): _engine_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_engine_value(item) for item in value]
    return value


def construct_from_supplied_market_facts(
    strategy_context: Mapping[str, Any],
    option_chain: Sequence[Mapping[str, Any]],
    *,
    config: OptionsConstructionConfig | None = None,
) -> dict[str, Any]:
    """Run the reviewed engine against caller-supplied current facts only.

    The wrapper deliberately does not fetch an option chain or accept an SDK
    client.  Decimal-to-float conversion happens only at the existing engine's
    numerical boundary; callers normalize returned money back to Decimal.
    """
    if not isinstance(strategy_context, Mapping) or not isinstance(option_chain, Sequence):
        raise TypeError("Supplied construction facts must use mapping/sequence contracts.")
    payload = _engine_value(deepcopy(dict(strategy_context)))
    chain = _engine_value(deepcopy([dict(item) for item in option_chain]))
    return OptionsConstructionEngine(config).construct(payload, chain)


def construct_with_scored_alternatives(
    strategy_context: Mapping[str, Any],
    option_chain: Sequence[Mapping[str, Any]],
    *,
    config: OptionsConstructionConfig | None = None,
) -> dict[str, Any]:
    """Return the unchanged winner plus authoritative in-memory scored rows."""
    if not isinstance(strategy_context, Mapping) or not isinstance(option_chain, Sequence):
        raise TypeError("Supplied construction facts must use mapping/sequence contracts.")
    payload = _engine_value(deepcopy(dict(strategy_context)))
    chain = _engine_value(deepcopy([dict(item) for item in option_chain]))
    return OptionsConstructionEngine(config).construct(
        payload, chain, include_scored_candidates=True
    )


def construct_with_analytical_alternatives(
    strategy_context: Mapping[str, Any],
    option_chain: Sequence[Mapping[str, Any]],
    *,
    config: OptionsConstructionConfig | None = None,
) -> dict[str, Any]:
    """Return a broader comparison set while retaining the normal winner."""
    payload = _engine_value(deepcopy(dict(strategy_context)))
    chain = _engine_value(deepcopy([dict(item) for item in option_chain]))
    normal = OptionsConstructionEngine(config).construct(
        payload, chain, include_scored_candidates=True
    )
    if not normal.get("candidate_id"):
        return normal
    analytical = OptionsConstructionEngine(config).construct(
        payload, chain, include_scored_candidates=True, analytical_comparison=True
    )
    if not analytical.get("scored_candidates"):
        return normal
    selected_id = normal["candidate_id"]
    rows = [dict(item, selected=item.get("candidate_id") == selected_id) for item in analytical["scored_candidates"]]
    if selected_id not in {item.get("candidate_id") for item in rows}:
        rows.extend(dict(item, selected=True) for item in normal["scored_candidates"] if item.get("candidate_id") == selected_id)
    result = dict(normal)
    result["scored_candidates"] = rows
    result["analytical_comparison"] = True
    return result
