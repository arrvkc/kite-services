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


USER_FIRST_FAMILIES = frozenset({
    "BEAR_CALL_SPREAD", "BULL_PUT_SPREAD", "BEAR_PUT_SPREAD", "IRON_CONDOR",
})


def construct_user_first(
    strategy_context, option_chain, *, config=None,
    selected_buy_strike=None, selected_sell_strike=None, selected_legs=None,
):
    """Unranked manual evaluation over exact supplied facts, never recommendations.

    Bull Call is deliberately excluded: its frozen ATMS manual path is unchanged.
    Only enumeration and the credit-efficiency preference differ from automatic
    construction. All market validation, liquidity, pricing and payoff remain
    owned by this engine. Four-leg selection is exact, never a Cartesian search.
    """
    import math
    from types import SimpleNamespace
    from .models import CandidateLeg
    from .constants import LIQUIDITY_CHECK_PASSED, COMPLETED_SESSION_LIQUIDITY_CHECK_PASSED

    payload = _engine_value(deepcopy(dict(strategy_context)))
    chain = _engine_value(deepcopy([dict(item) for item in option_chain]))
    family = payload.get("strategy_family")
    if family not in USER_FIRST_FAMILIES:
        raise ValueError("USER_FIRST is unsupported for this family.")
    engine = OptionsConstructionEngine(config)
    result = dict(strategy_family=family, manual_policy="USER_FIRST",
                  manual_candidates=[], eligible_buy_strikes=[], eligible_sell_strikes=[],
                  leg_options={}, manual_expiry=None,
                  manual_reference=payload.get("underlying_spot_price"), manual_rejections=[])

    def reject(code):
        result["manual_rejections"].append({"code": code})
        return result

    failure = engine._validate_inputs(payload, chain)
    if failure:
        return reject(failure[0])
    contracts = engine._normalize_option_chain(chain)
    if isinstance(contracts, tuple):
        return reject(contracts[0])
    failure = engine._validate_option_chain(payload, contracts, engine._new_audit(payload, chain))
    if failure:
        return reject(failure[0])
    expiry, code = engine._select_expiry(payload, contracts)
    if expiry is None:
        return reject(code)
    result["manual_expiry"] = expiry
    contracts = [c for c in contracts if c.expiry == expiry]
    keys = [(c.option_type, c.strike) for c in contracts]
    identities = [(c.tradingsymbol, str(c.instrument_token)) for c in contracts]
    if len(set(keys)) != len(keys) or len({i[0] for i in identities}) != len(identities) or len({i[1] for i in identities}) != len(identities):
        return reject("AMBIGUOUS_INSTRUMENT")
    passed = {LIQUIDITY_CHECK_PASSED, COMPLETED_SESSION_LIQUIDITY_CHECK_PASSED}
    liquid = []
    for c in contracts:
        probe = engine._candidate(family, expiry, (CandidateLeg("LONG_LEG", "BUY", c),), 0, {})
        if engine._liquidity_code(probe)[0] in passed:
            liquid.append(c)

    def strike(value):
        if value in (None, ""):
            return None
        if isinstance(value, bool):
            raise ValueError("Invalid selected strike.")
        number = float(value)
        if not math.isfinite(number) or number <= 0:
            raise ValueError("Invalid selected strike.")
        return number

    candidates = []
    if family == "IRON_CONDOR":
        if selected_buy_strike not in (None, "") or selected_sell_strike not in (None, ""):
            raise ValueError("Choose the four explicit Iron Condor legs.")
        fields = (("buy_put", "PUT_LONG_WING", "BUY", "PE"),
                  ("sell_put", "PUT_SHORT", "SELL", "PE"),
                  ("sell_call", "CALL_SHORT", "SELL", "CE"),
                  ("buy_call", "CALL_LONG_WING", "BUY", "CE"))
        selection = selected_legs or {}
        if not isinstance(selection, dict) or set(selection) - {f[0] for f in fields}:
            raise ValueError("Invalid Iron Condor selection.")
        chosen, previous, missing = [], None, False
        for key, role, side, kind in fields:
            choices = [c for c in liquid if c.option_type == kind and (previous is None or c.strike > previous)]
            result["leg_options"][key] = sorted(c.strike for c in choices) if not missing else []
            selected = strike(selection.get(key))
            if selected is None:
                missing = True
                continue
            if missing or (previous is not None and selected <= previous):
                raise ValueError("Choose Buy Put < Sell Put < Sell Call < Buy Call in order.")
            contract = next((c for c in choices if c.strike == selected), None)
            if contract is None:
                return reject("SELECTED_LEG_UNAVAILABLE")
            chosen.append(CandidateLeg(role, side, contract))
            previous = selected
        if len(chosen) == 4:
            candidates.append(engine._candidate(family, expiry, tuple(chosen), 0, {}))
    else:
        if selected_legs:
            raise ValueError("Four-leg selection is unsupported for this family.")
        buy, sell = strike(selected_buy_strike), strike(selected_sell_strike)
        credit = family != "BEAR_PUT_SPREAD"
        kind = "CE" if family == "BEAR_CALL_SPREAD" else "PE"
        calls = [c for c in liquid if c.option_type == kind]
        anchor, companion = (sell, buy) if credit else (buy, sell)
        result["eligible_sell_strikes" if credit else "eligible_buy_strikes"] = sorted(c.strike for c in calls)
        if companion is not None and anchor is None:
            raise ValueError("Choose the anchor leg first.")
        if buy is not None and sell is not None and (
            buy <= sell if family in {"BEAR_CALL_SPREAD", "BEAR_PUT_SPREAD"} else buy >= sell
        ):
            raise ValueError("Invalid protective-leg strike ordering.")
        if anchor is not None:
            for a in calls:
                if a.strike != anchor:
                    continue
                for b in calls:
                    if not (b.strike > a.strike if family == "BEAR_CALL_SPREAD" else b.strike < a.strike):
                        continue
                    if companion is not None and b.strike != companion:
                        continue
                    legs = (CandidateLeg("SHORT_LEG", "SELL", a), CandidateLeg("LONG_LEG", "BUY", b)) if credit else (
                        CandidateLeg("LONG_LEG", "BUY", a), CandidateLeg("SHORT_LEG", "SELL", b))
                    candidates.append(engine._candidate(family, expiry, legs, 0, {}))
    # No strike-mode choice, recommendation generation, score or rank is called.
    for candidate in candidates:
        code, _ = engine._liquidity_code(candidate)
        if code not in passed:
            result["manual_rejections"].append({"candidate_id": candidate.candidate_id, "code": code})
            continue
        economics, code = engine._price_and_risk(payload, candidate, policy="USER_FIRST")
        if economics is None or not all(math.isfinite(v) and v > 0 for v in (
            economics.net_premium, economics.max_profit, economics.max_loss,
            economics.max_profit_per_lot, economics.max_loss_per_lot,
        )):
            result["manual_rejections"].append({"candidate_id": candidate.candidate_id, "code": code if economics is None else "INVALID_PAYOFF"})
            continue
        # Reuse the existing economics/leg serializer without executing scoring.
        row = engine._scored_candidate_output(SimpleNamespace(candidate=candidate, economics=economics, construction_score=None), "")
        for key in ("construction_score", "selected", "roi"):
            row.pop(key, None)
        if family == "IRON_CONDOR":
            # Unequal manual wings may have only one crossing: a credit larger
            # than a wing's width leaves that entire tail profitable. Preserve
            # engine breakeven arithmetic; retain only crossings on each wing.
            put_long, put_short, call_short, call_long = [leg.contract.strike for leg in candidate.legs]
            lower, upper = row["breakeven_prices"]
            row["breakeven_prices"] = ([lower] if put_long <= lower <= put_short else []) + ([upper] if call_short <= upper <= call_long else [])
        result["manual_candidates"].append(row)
    return result
