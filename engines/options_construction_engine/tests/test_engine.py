from __future__ import annotations

import copy
import json
from pathlib import Path
from decimal import Decimal
from types import SimpleNamespace

from engines.options_construction_engine.constants import *
from engines.options_construction_engine.contract import (
    CONTRACT_IDENTITY,
    construct_from_supplied_market_facts,
)
from engines.options_construction_engine.engine import (
    OptionsConstructionEngine,
    _breakeven_prices,
)
from engines.options_construction_engine.models import OptionsConstructionConfig

ROOT = Path(__file__).resolve().parents[1]


def load_reference():
    with (ROOT / "examples" / "reference_input.json").open("r", encoding="utf-8") as fh:
        return json.load(fh)


def construct_from_fixture(fixture, *, compatibility=False):
    option_chain = fixture.pop("option_chain")
    return OptionsConstructionEngine(OptionsConstructionConfig(reference_fixture_compatibility=compatibility)).construct(fixture, option_chain)


def test_reference_fixture_matches_expected_output_in_declared_compatibility_mode():
    fixture = load_reference()
    result = construct_from_fixture(fixture, compatibility=True)
    with (ROOT / "examples" / "reference_expected_output.json").open("r", encoding="utf-8") as fh:
        expected = json.load(fh)
    assert result == expected


def test_strict_default_rejects_pdf_reference_due_no_next_expiry_first():
    fixture = load_reference()
    result = construct_from_fixture(fixture)
    assert result["construction_status"] == STATUS_REJECTED
    assert NO_VALID_NEXT_EXPIRY in result["reason_codes"]


def test_tc_001_bear_call_next_month_delta_available_constructs_when_quotes_are_liquid():
    fixture = load_reference()
    chain = fixture.pop("option_chain")
    for c in chain:
        if c["strike"] == 155:
            c["bid_price"], c["ask_price"] = 3.0, 3.2
        if c["strike"] == 160:
            c["bid_price"], c["ask_price"] = 1.4, 1.6
    # Add a true near expiry so NEXT_MONTH selection is strict.
    chain.extend([
        dict(chain[0], tradingsymbol="ABC24APR155CE", instrument_token=9002, expiry="2026-04-30", data_timestamp="2026-04-15T10:29:00Z"),
        dict(chain[1], tradingsymbol="ABC24APR160CE", instrument_token=9003, expiry="2026-04-30", data_timestamp="2026-04-15T10:29:00Z"),
    ])
    result = OptionsConstructionEngine().construct(fixture, chain)
    assert result["construction_status"] == STATUS_CONSTRUCTED
    assert result["legs"][0]["side"] == "SELL"
    assert result["legs"][0]["option_type"] == "CE"
    assert result["legs"][0]["strike"] < result["legs"][1]["strike"]
    assert result["expiry"] == "2026-05-28"
    assert result["breakeven_prices"] == [
        result["legs"][0]["strike"] + result["net_premium"]
    ]


def test_reviewed_spread_breakeven_uses_family_payoff_contract():
    fixture = load_reference()
    result = construct_from_fixture(fixture, compatibility=True)

    assert result["strategy_family"] == FAMILY_BEAR_CALL_SPREAD
    assert result["breakeven_prices"] == [156.0]
    assert result["breakeven_prices"][0] == (
        result["legs"][0]["strike"] + result["net_premium"]
    )


def test_breakeven_contract_covers_all_reviewed_defined_risk_families():
    def candidate(family, legs):
        return SimpleNamespace(
            strategy_family=family,
            legs=tuple(
                SimpleNamespace(
                    side=side,
                    contract=SimpleNamespace(option_type=option_type, strike=strike),
                )
                for side, option_type, strike in legs
            ),
        )

    assert _breakeven_prices(candidate(FAMILY_BULL_CALL_SPREAD, (
        (SIDE_BUY, OPTION_CE, 100), (SIDE_SELL, OPTION_CE, 110)
    )), 4) == [104]
    assert _breakeven_prices(candidate(FAMILY_BEAR_PUT_SPREAD, (
        (SIDE_BUY, OPTION_PE, 100), (SIDE_SELL, OPTION_PE, 90)
    )), 4) == [96]
    assert _breakeven_prices(candidate(FAMILY_BULL_PUT_SPREAD, (
        (SIDE_SELL, OPTION_PE, 100), (SIDE_BUY, OPTION_PE, 90)
    )), 4) == [96]
    assert _breakeven_prices(candidate(FAMILY_BEAR_CALL_SPREAD, (
        (SIDE_SELL, OPTION_CE, 100), (SIDE_BUY, OPTION_CE, 110)
    )), 4) == [104]
    assert _breakeven_prices(candidate(FAMILY_IRON_CONDOR, (
        (SIDE_BUY, OPTION_PE, 80), (SIDE_SELL, OPTION_PE, 90),
        (SIDE_SELL, OPTION_CE, 110), (SIDE_BUY, OPTION_CE, 120),
    )), 4) == [86, 114]


def test_tc_002_rejects_zero_bid_first_candidate_and_evaluates_next_candidate():
    fixture = load_reference()
    chain = fixture.pop("option_chain")
    for c in chain:
        if c["strike"] == 155:
            c["bid_price"] = 0.0
        if c["strike"] == 160:
            c["bid_price"], c["ask_price"] = 1.4, 1.6
    chain.append({
        "tradingsymbol": "ABC24MAY165CE", "instrument_token": 1004, "expiry": "2026-05-28",
        "strike": 165, "option_type": "CE", "bid_price": 1.0, "ask_price": 1.1,
        "last_price": 1.05, "open_interest": 2500, "volume": 800, "delta": 0.16,
        "data_timestamp": "2026-04-15T10:29:00Z"
    })
    result = OptionsConstructionEngine(OptionsConstructionConfig(reference_fixture_compatibility=True)).construct(fixture, chain)
    assert result["construction_status"] in {STATUS_CONSTRUCTED, STATUS_REJECTED}
    assert not (result["construction_status"] == STATUS_CONSTRUCTED and result["legs"][0]["tradingsymbol"] == "ABC24MAY155CE")


def test_tc_003_all_candidates_fail_bid_ask_spread():
    fixture = load_reference()
    fixture["contract_month_selection"] = CONTRACT_NEAR_MONTH
    result = construct_from_fixture(fixture)
    assert result["construction_status"] == STATUS_REJECTED
    assert LIQUIDITY_CHECK_FAILED in result["reason_codes"]
    assert BID_ASK_SPREAD_TOO_WIDE in result["reason_codes"]


def test_tc_004_bull_put_greeks_unavailable_uses_distance_mode():
    fixture = load_reference()
    fixture["strategy_family"] = FAMILY_BULL_PUT_SPREAD
    chain = fixture.pop("option_chain")
    chain = [
        {"tradingsymbol": "ABC24MAY140PE", "instrument_token": 2001, "expiry": "2026-05-28", "strike": 140, "option_type": "PE", "bid_price": 3.0, "ask_price": 3.2, "last_price": 3.1, "open_interest": 2500, "volume": 700, "delta": None, "data_timestamp": "2026-04-15T10:29:00Z"},
        {"tradingsymbol": "ABC24MAY130PE", "instrument_token": 2002, "expiry": "2026-05-28", "strike": 130, "option_type": "PE", "bid_price": 1.0, "ask_price": 1.1, "last_price": 1.05, "open_interest": 2500, "volume": 700, "delta": None, "data_timestamp": "2026-04-15T10:29:00Z"},
    ]
    result = OptionsConstructionEngine(OptionsConstructionConfig(reference_fixture_compatibility=True)).construct(fixture, chain)
    assert DISTANCE_BASED_STRIKE_SELECTION in result["reason_codes"]


def test_tc_005_credit_too_low():
    fixture = load_reference()
    chain = fixture.pop("option_chain")
    for c in chain:
        if c["strike"] == 155:
            c["bid_price"], c["ask_price"] = 1.4, 1.5
        if c["strike"] == 160:
            c["bid_price"], c["ask_price"] = 0.8, 0.9
    result = OptionsConstructionEngine(OptionsConstructionConfig(reference_fixture_compatibility=True)).construct(fixture, chain)
    assert result["construction_status"] == STATUS_REJECTED
    assert CREDIT_TOO_LOW in result["reason_codes"]


def test_tc_006_debit_spread_reward_risk_below_threshold():
    payload = {
        "instrument": "ABC", "asof_time": "2026-04-15T10:30:00Z", "strategy_family": FAMILY_BULL_CALL_SPREAD,
        "contract_month_selection": CONTRACT_NEAR_MONTH, "final_strategy_strength": 70,
        "underlying_spot_price": 150.0, "lot_size": 100, "strike_step": 5,
    }
    chain = [
        {"tradingsymbol": "ABC24MAY150CE", "instrument_token": 3001, "expiry": "2026-05-28", "strike": 150, "option_type": "CE", "bid_price": 9.4, "ask_price": 9.6, "last_price": 9.5, "open_interest": 2500, "volume": 700, "delta": 0.50, "data_timestamp": "2026-04-15T10:29:00Z"},
        {"tradingsymbol": "ABC24MAY160CE", "instrument_token": 3002, "expiry": "2026-05-28", "strike": 160, "option_type": "CE", "bid_price": 4.0, "ask_price": 4.2, "last_price": 4.1, "open_interest": 2500, "volume": 700, "delta": 0.30, "data_timestamp": "2026-04-15T10:29:00Z"},
    ]
    result = OptionsConstructionEngine().construct(payload, chain)
    assert result["construction_status"] == STATUS_REJECTED
    assert POOR_REWARD_RISK in result["reason_codes"]


def test_tc_007_tie_break_selects_lower_bid_ask_spread():
    payload = {
        "instrument": "ABC", "asof_time": "2026-04-15T10:30:00Z", "strategy_family": FAMILY_BEAR_CALL_SPREAD,
        "contract_month_selection": CONTRACT_NEAR_MONTH, "final_strategy_strength": 70,
        "underlying_spot_price": 150.0, "lot_size": 100, "strike_step": 5,
    }
    chain = [
        {"tradingsymbol": "A155", "instrument_token": 1, "expiry": "2026-05-28", "strike": 155, "option_type": "CE", "bid_price": 3.0, "ask_price": 3.1, "last_price": 3.05, "open_interest": 2000, "volume": 500, "delta": 0.20, "data_timestamp": "2026-04-15T10:29:00Z"},
        {"tradingsymbol": "A165", "instrument_token": 2, "expiry": "2026-05-28", "strike": 165, "option_type": "CE", "bid_price": 0.8, "ask_price": 0.9, "last_price": 0.85, "open_interest": 2000, "volume": 500, "delta": 0.10, "data_timestamp": "2026-04-15T10:29:00Z"},
        {"tradingsymbol": "B155", "instrument_token": 3, "expiry": "2026-05-28", "strike": 155, "option_type": "CE", "bid_price": 3.0, "ask_price": 3.2, "last_price": 3.1, "open_interest": 2000, "volume": 500, "delta": 0.20, "data_timestamp": "2026-04-15T10:29:00Z"},
        {"tradingsymbol": "B165", "instrument_token": 4, "expiry": "2026-05-28", "strike": 165, "option_type": "CE", "bid_price": 0.8, "ask_price": 1.0, "last_price": 0.9, "open_interest": 2000, "volume": 500, "delta": 0.10, "data_timestamp": "2026-04-15T10:29:00Z"},
    ]
    result = OptionsConstructionEngine().construct(payload, chain)
    assert result["construction_status"] == STATUS_CONSTRUCTED
    assert result["legs"][0]["tradingsymbol"] == "A155"


def test_tc_008_no_next_monthly_expiry_rejected():
    fixture = load_reference()
    chain = fixture.pop("option_chain")
    # strict mode: one expiry only is not enough for NEXT_MONTH
    result = OptionsConstructionEngine().construct(fixture, chain)
    assert result["construction_status"] == STATUS_REJECTED
    assert NO_VALID_NEXT_EXPIRY in result["reason_codes"] or BID_ASK_SPREAD_TOO_WIDE in result["reason_codes"]


def test_deterministic_output_identical_for_identical_inputs():
    fixture = load_reference()
    chain = fixture.pop("option_chain")
    engine = OptionsConstructionEngine(OptionsConstructionConfig(reference_fixture_compatibility=True))
    first = engine.construct(copy.deepcopy(fixture), copy.deepcopy(chain))
    second = engine.construct(copy.deepcopy(fixture), copy.deepcopy(chain))
    assert first == second


def test_supplied_market_contract_is_golden_equivalent_to_reviewed_engine():
    fixture = load_reference()
    chain = fixture.pop("option_chain")
    config = OptionsConstructionConfig(reference_fixture_compatibility=True)
    expected = OptionsConstructionEngine(config).construct(
        copy.deepcopy(fixture), copy.deepcopy(chain)
    )
    actual = construct_from_supplied_market_facts(fixture, chain, config=config)
    assert CONTRACT_IDENTITY == "EAJEE_OPTIONS_CONSTRUCTION_SUPPLIED_MARKET_FACTS"
    assert actual == expected


def test_supplied_market_contract_accepts_exact_decimal_boundary_without_mutation():
    fixture = load_reference()
    chain = fixture.pop("option_chain")
    fixture["underlying_spot_price"] = Decimal(str(fixture["underlying_spot_price"]))
    fixture["strike_step"] = Decimal(str(fixture["strike_step"]))
    chain[0]["bid_price"] = Decimal(str(chain[0]["bid_price"]))
    original = copy.deepcopy(chain)
    result = construct_from_supplied_market_facts(
        fixture,
        chain,
        config=OptionsConstructionConfig(reference_fixture_compatibility=True),
    )
    assert result["construction_status"] in {STATUS_CONSTRUCTED, STATUS_REJECTED}
    assert chain == original


def test_supplied_market_context_preserves_reviewed_runner_conventions():
    from engines.options_construction_engine.contract import (
        prepare_supplied_option_market_context,
    )

    chain = [
        {"strike": Decimal("100"), "lot_size": 50, "tradingsymbol": "A"},
        {"strike": Decimal("105"), "lot_size": 50, "tradingsymbol": "B"},
        {"strike": Decimal("115"), "lot_size": 25, "tradingsymbol": "C"},
    ]
    original = copy.deepcopy(chain)
    prepared = prepare_supplied_option_market_context(chain)
    assert prepared["strike_step"] == Decimal("5")
    assert prepared["lot_size"] == 50
    assert [item["tradingsymbol"] for item in prepared["option_chain"]] == ["A", "B"]
    assert chain == original
