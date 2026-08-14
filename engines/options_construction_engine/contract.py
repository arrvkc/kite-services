"""Stable pure boundary for reviewed five-family option construction.

The caller supplies one coherent current-market observation.  This module has
no broker client, credentials, network call or execution surface; it delegates
unchanged financial rules to :class:`OptionsConstructionEngine`.
"""

from __future__ import annotations

from copy import deepcopy
from decimal import Decimal
from typing import Any, Mapping, Sequence

from .engine import OptionsConstructionEngine
from .models import OptionsConstructionConfig


CONTRACT_IDENTITY = "EAJEE_OPTIONS_CONSTRUCTION_SUPPLIED_MARKET_FACTS"


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
