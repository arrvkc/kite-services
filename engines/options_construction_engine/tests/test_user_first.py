from copy import deepcopy
import pytest

from engines.options_construction_engine.contract import construct_user_first
from engines.options_construction_engine.engine import OptionsConstructionEngine
from engines.options_construction_engine.models import OptionsConstructionConfig


def market(family, completed=False):
    payload = dict(instrument="TEST", strategy_family=family, contract_month_selection="NEAR_MONTH",
                   final_strategy_strength=70, underlying_spot_price=1000, lot_size=50, strike_step=20,
                   asof_time="2026-09-11T10:00:00+05:30")
    values = [("CE", 1000, 5), ("CE", 1040, 4.5), ("CE", 1060, 4), ("CE", 1200, 2),
              ("PE", 700, 1), ("PE", 800, 2), ("PE", 900, 3), ("PE", 1000, 5), ("CE", 1400, 1)]
    chain = [dict(tradingsymbol=f"TEST{k}{kind}", instrument_token=k+(10000 if kind=="PE" else 0), expiry="2026-09-29",
                  strike=k, option_type=kind, bid_price=p, ask_price=p+.05, last_price=p,
                  open_interest=2000, volume=1000, data_timestamp=payload['asof_time'],
                  underlying="TEST", lot_size=50, delta=.99, delta_source_verified=True,
                  delta_source="test", delta_timestamp=payload['asof_time']) for kind,k,p in values]
    if completed:
        for c in chain:
            c.update(bid_price=None, ask_price=None, historical_reference_price=c['last_price'],
                     historical_session_identity='NSE:2026-09-10', historical_last_trade_at='2026-09-10T15:20:00+05:30')
    return payload, chain, OptionsConstructionConfig(liquidity_mode='COMPLETED_SESSION_HISTORICAL' if completed else 'LIVE_STRICT')


@pytest.mark.parametrize('family,buy,sell', [('BEAR_CALL_SPREAD',1200,1000),('BULL_PUT_SPREAD',800,1000),('BEAR_PUT_SPREAD',1000,800)])
@pytest.mark.parametrize('completed',[False,True])
def test_vertical_user_first_ignores_auto_targets_preserves_engine_payoff(family,buy,sell,completed,monkeypatch):
    p,c,config=market(family,completed)
    def forbidden(*args,**kwargs): pytest.fail('manual must never run recommendation targeting or scoring')
    for name in ('_generate_candidates','_choose_strike_mode','_score_candidate','_rank_scored'):
        monkeypatch.setattr(OptionsConstructionEngine,name,forbidden)
    result=construct_user_first(p,c,config=config,selected_buy_strike=buy,selected_sell_strike=sell)
    row,=result['manual_candidates']
    assert [(x['side'],x['strike']) for x in row['legs']]==([('BUY',buy),('SELL',sell)] if family=='BEAR_PUT_SPREAD' else [('SELL',sell),('BUY',buy)])
    assert row['net_premium']==pytest.approx(3 if completed else 3.05 if family=='BEAR_PUT_SPREAD' else 2.95)
    assert row['max_profit_per_lot']>0 and row['max_loss_per_lot']>0
    assert row['max_profit_per_lot']+row['max_loss_per_lot']==10000
    assert row['reward_risk_ratio']==row['max_profit_per_lot']/row['max_loss_per_lot']
    assert not {'construction_score','roi','probability_of_profit'} & row.keys()
    expected_be=buy-row['net_premium'] if family=='BEAR_PUT_SPREAD' else sell+row['net_premium'] if family=='BEAR_CALL_SPREAD' else sell-row['net_premium']
    assert row['breakeven_prices']==[expected_be]


def test_credit_floor_remains_automatic_only():
    p,c,_=market('BEAR_CALL_SPREAD')
    for item in c: item['delta']=None
    automatic=OptionsConstructionEngine().construct(p,c,include_scored_candidates=True)
    assert 'CREDIT_TOO_LOW' in automatic['reason_codes']
    row,=construct_user_first(p,c,selected_sell_strike=1040,selected_buy_strike=1060)['manual_candidates']
    assert row['net_premium']==.45 and row['credit_or_debit_to_width']==.0225
    assert OptionsConstructionEngine().construct(p,c,include_scored_candidates=True)==automatic


@pytest.mark.parametrize('completed',[False,True])
def test_condor_exact_unequal_wings_no_auto_distance_width_or_floor(completed):
    p,c,config=market('IRON_CONDOR',completed)
    selection=dict(buy_put=700,sell_put=900,sell_call=1000,buy_call=1400)
    result=construct_user_first(p,c,config=config,selected_legs=selection)
    row,=result['manual_candidates']
    assert [x['strike'] for x in row['legs']]==[700,900,1000,1400]
    credit=6 if completed else 5.9
    assert row['net_premium']==credit
    assert row['max_profit_per_lot']==credit*50
    assert row['max_loss_per_lot']==(400-credit)*50
    assert row['breakeven_prices']==[900-credit,1000+credit]
    assert not construct_user_first(p,c,config=config,selected_legs={'buy_put':700})['manual_candidates']


def test_condor_asymmetric_profitable_tail_has_one_actual_breakeven():
    p,c,config=market('IRON_CONDOR')
    # Narrow put wing remains profitable everywhere when total credit > its width.
    for item in c:
        if item['option_type']=='PE' and item['strike']==800: item['strike']=899; item['tradingsymbol']='TEST899PE'
        if item['option_type']=='CE' and item['strike']==1000: item.update(bid_price=10,ask_price=10.05,last_price=10)
    row,=construct_user_first(p,c,config=config,selected_legs=dict(buy_put=899,sell_put=900,sell_call=1000,buy_call=1400))['manual_candidates']
    assert row['breakeven_prices']==[1009.9]


@pytest.mark.parametrize('field,value', [('volume',0),('open_interest',499),('bid_price',0),('ask_price',20),('data_timestamp','2026-09-10T10:00:00+05:30'),('lot_size',51),('underlying','OTHER')])
def test_hard_market_validity_not_weakened(field,value):
    p,c,config=market('BEAR_CALL_SPREAD')
    next(x for x in c if x['option_type']=='CE' and x['strike']==1200)[field]=value
    assert not construct_user_first(p,c,config=config,selected_sell_strike=1000,selected_buy_strike=1200)['manual_candidates']


@pytest.mark.parametrize('buy,sell',[(1000,1200),(1000,1000)])
def test_invalid_geometry_rejected(buy,sell):
    p,c,config=market('BEAR_CALL_SPREAD')
    with pytest.raises(ValueError): construct_user_first(p,c,config=config,selected_buy_strike=buy,selected_sell_strike=sell)


def test_invalid_credit_and_nonpositive_payoff_fail_closed():
    p,c,config=market('BEAR_CALL_SPREAD')
    for bid,ask in [(6,6.05),(.01,.02)]:
        modified=deepcopy(c)
        long=next(x for x in modified if x['option_type']=='CE' and x['strike']==1060)
        long.update(bid_price=bid,ask_price=ask)
        if bid==.01:
            short=next(x for x in modified if x['option_type']=='CE' and x['strike']==1040)
            short.update(bid_price=30,ask_price=30.05)
        assert not construct_user_first(p,modified,config=config,selected_sell_strike=1040,selected_buy_strike=1060)['manual_candidates']


def test_bull_call_cannot_opt_into_user_first_policy():
    p,c,config=market('BULL_CALL_SPREAD')
    with pytest.raises(ValueError): construct_user_first(p,c,config=config)
