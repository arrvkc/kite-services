#!/usr/bin/env python3
"""
Strategy Risk Monitor - Short Strangle Monitor

Purpose:
- Takes Zerodha user id and symbol.
- Reads live Kite positions.
- Detects short strangles for the given symbol.
- Fetches latest spot, futures, and option LTPs.
- Calculates IV and Greeks using forward/FUT based Black-Scholes.
- Monitors risk every N seconds throughout the trading day.
- Prints and logs HOLD / WATCH / EXIT decisions.

Important:
- This file does NOT place exit orders.
- It is a monitor and decision logger only.

Example:
    PYTHONPATH=.:services python strategy_risk_monitor.py --user-id OMK569 --symbol TRENT

Optional:
    PYTHONPATH=.:services python strategy_risk_monitor.py --user-id OMK569 --symbol TRENT --interval 300
"""

from __future__ import annotations

import argparse
import csv
import math
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, date, time as dtime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from kiteconnect import KiteConnect
from scipy.optimize import brentq
from scipy.stats import norm

from kite_credentials_service import get_kite_credentials


# ============================================================
# Configuration
# ============================================================

DEFAULT_RISK_FREE_RATE = 0.065
DEFAULT_INTERVAL_SECONDS = 300
DEFAULT_MARKET_OPEN = "09:15"
DEFAULT_MARKET_CLOSE = "15:30"
DEFAULT_LOG_DIR = "logs"

# Risk thresholds
DELTA_WATCH = 0.20
DELTA_EXIT = 0.25

DISTANCE_WATCH_PCT = 0.025     # 2.5%
DISTANCE_EXIT_PCT = 0.010      # 1.0%

IV_WATCH_POINTS = 2.0          # vol points
IV_EXIT_POINTS = 4.0           # vol points

LOSS_WATCH_MULTIPLE = 1.25
LOSS_EXIT_MULTIPLE = 1.75


# ============================================================
# Models
# ============================================================

@dataclass(frozen=True)
class OptionLeg:
    tradingsymbol: str
    underlying: str
    expiry_code: str
    expiry_date: date
    strike: int
    option_type: str       # CE / PE
    quantity: int          # Kite signed quantity
    average_price: float
    last_price: float


@dataclass(frozen=True)
class FutureLeg:
    tradingsymbol: str
    underlying: str
    expiry_code: str
    expiry_date: date
    quantity: int
    average_price: float
    last_price: float


@dataclass(frozen=True)
class ShortStrangle:
    underlying: str
    expiry_code: str
    expiry_date: date
    short_put: OptionLeg
    short_call: OptionLeg
    quantity: int


@dataclass(frozen=True)
class NakedOption:
    underlying: str
    expiry_code: str
    expiry_date: date
    leg: OptionLeg
    quantity: int


@dataclass(frozen=True)
class CoveredCall:
    underlying: str
    expiry_code: str
    expiry_date: date
    leg: OptionLeg
    quantity: int
    cover_type: str      # FUTURE / STOCK


@dataclass(frozen=True)
class CoveredPut:
    underlying: str
    expiry_code: str
    expiry_date: date
    leg: OptionLeg
    quantity: int
    cover_type: str      # FUTURE


@dataclass(frozen=True)
class CreditSpread:
    underlying: str
    expiry_code: str
    expiry_date: date
    spread_type: str     # BEAR_CALL / BULL_PUT
    short_leg: OptionLeg
    long_leg: OptionLeg
    quantity: int


@dataclass
class LegGreeks:
    iv_pct: float
    delta: float
    gamma: float
    theta: float
    vega: float


@dataclass
class StrategySnapshot:
    timestamp: str
    user_id: str
    symbol: str
    spot: float
    future: float
    short_put_symbol: str
    short_call_symbol: str
    short_put_strike: int
    short_call_strike: int
    short_put_ltp: float
    short_call_ltp: float
    short_put_iv: float
    short_call_iv: float
    net_delta: float
    net_gamma: float
    net_theta: float
    net_vega: float
    dist_put_pct: float
    dist_call_pct: float
    entry_premium_unit: float
    current_premium_unit: float
    pnl_unit: float
    pnl_scaled: float
    iv_change_points: float
    delta_change: float
    gamma_change: float
    pnl_change: float
    risk_score: float
    decision: str
    reason_codes: List[str]


# ============================================================
# Symbol parsing
# ============================================================

MONTHS = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4,
    "MAY": 5, "JUN": 6, "JUL": 7, "AUG": 8,
    "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
}

OPTION_RE = re.compile(r"^([A-Z]+)(\d{2})([A-Z]{3})(\d+)(CE|PE)$")
FUTURE_RE = re.compile(r"^([A-Z]+)(\d{2})([A-Z]{3})FUT$")


def parse_expiry_date(year2: str, mon: str) -> date:
    """
    Converts 26MAY to an approximate expiry date.

    For production monitoring, we prefer exact expiry from Kite instruments.
    This fallback uses the 26th only when exact instrument data is unavailable.
    """
    year = 2000 + int(year2)
    month = MONTHS[mon]
    return date(year, month, 26)


def parse_option_symbol(tradingsymbol: str) -> Optional[Tuple[str, str, date, int, str]]:
    m = OPTION_RE.match(tradingsymbol)
    if not m:
        return None

    underlying, yy, mon, strike, option_type = m.groups()
    expiry_code = f"{yy}{mon}"
    expiry_dt = parse_expiry_date(yy, mon)
    return underlying, expiry_code, expiry_dt, int(strike), option_type


def parse_future_symbol(tradingsymbol: str) -> Optional[Tuple[str, str, date]]:
    m = FUTURE_RE.match(tradingsymbol)
    if not m:
        return None

    underlying, yy, mon = m.groups()
    expiry_code = f"{yy}{mon}"
    expiry_dt = parse_expiry_date(yy, mon)
    return underlying, expiry_code, expiry_dt


# ============================================================
# Kite setup
# ============================================================

def make_kite(user_id: str) -> KiteConnect:
    api_key, access_token = get_kite_credentials(user_id)
    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    return kite


# ============================================================
# Position detection
# ============================================================

def fetch_positions(kite: KiteConnect) -> List[dict]:
    return kite.positions()["net"]


def fetch_holdings_safe(kite: KiteConnect) -> List[dict]:
    try:
        return kite.holdings()
    except Exception:
        return []


def detect_short_option_underlyings(positions: List[dict]) -> List[str]:
    symbols = set()

    for p in positions:
        qty = int(p.get("quantity") or 0)
        if qty >= 0:
            continue

        ts = p.get("tradingsymbol", "")
        parsed = parse_option_symbol(ts)
        if not parsed:
            continue

        underlying, _, _, _, _ = parsed
        symbols.add(underlying)

    return sorted(symbols)


def detect_credit_spreads(
    positions: List[dict],
    symbol: str,
) -> List[CreditSpread]:
    """
    Detect defined-risk credit spreads.

    Bear Call Spread:
    - SELL CE at lower strike
    - BUY CE at higher strike

    Bull Put Spread:
    - SELL PE at higher strike
    - BUY PE at lower strike
    """
    symbol = symbol.upper().strip()

    short_by_exp_type: Dict[Tuple[str, str], List[OptionLeg]] = {}
    long_by_exp_type: Dict[Tuple[str, str], List[OptionLeg]] = {}

    for p in positions:
        qty = int(p.get("quantity") or 0)
        if qty == 0:
            continue

        ts = p.get("tradingsymbol", "")
        parsed = parse_option_symbol(ts)
        if not parsed:
            continue

        underlying, expiry_code, expiry_dt, strike, option_type = parsed
        if underlying != symbol:
            continue

        leg = OptionLeg(
            tradingsymbol=ts,
            underlying=underlying,
            expiry_code=expiry_code,
            expiry_date=expiry_dt,
            strike=strike,
            option_type=option_type,
            quantity=qty,
            average_price=float(p.get("average_price") or 0.0),
            last_price=float(p.get("last_price") or 0.0),
        )

        key = (expiry_code, option_type)

        if qty < 0:
            short_by_exp_type.setdefault(key, []).append(leg)
        else:
            long_by_exp_type.setdefault(key, []).append(leg)

    spreads: List[CreditSpread] = []

    for key in sorted(set(short_by_exp_type.keys()) & set(long_by_exp_type.keys())):
        expiry_code, option_type = key

        shorts = sorted(short_by_exp_type[key], key=lambda x: x.strike)
        longs = sorted(long_by_exp_type[key], key=lambda x: x.strike)

        remaining_short = {leg.tradingsymbol: abs(leg.quantity) for leg in shorts}
        remaining_long = {leg.tradingsymbol: abs(leg.quantity) for leg in longs}

        if option_type == "CE":
            # Bear call: short lower CE, long higher CE.
            for short_leg in shorts:
                for long_leg in longs:
                    if long_leg.strike <= short_leg.strike:
                        continue

                    qty = min(
                        remaining_short.get(short_leg.tradingsymbol, 0),
                        remaining_long.get(long_leg.tradingsymbol, 0),
                    )
                    if qty <= 0:
                        continue

                    spreads.append(
                        CreditSpread(
                            underlying=symbol,
                            expiry_code=expiry_code,
                            expiry_date=short_leg.expiry_date,
                            spread_type="BEAR_CALL",
                            short_leg=short_leg,
                            long_leg=long_leg,
                            quantity=qty,
                        )
                    )

                    remaining_short[short_leg.tradingsymbol] -= qty
                    remaining_long[long_leg.tradingsymbol] -= qty

                    if remaining_short[short_leg.tradingsymbol] <= 0:
                        break

        elif option_type == "PE":
            # Bull put: short higher PE, long lower PE.
            for short_leg in sorted(shorts, key=lambda x: x.strike, reverse=True):
                for long_leg in sorted(longs, key=lambda x: x.strike, reverse=True):
                    if long_leg.strike >= short_leg.strike:
                        continue

                    qty = min(
                        remaining_short.get(short_leg.tradingsymbol, 0),
                        remaining_long.get(long_leg.tradingsymbol, 0),
                    )
                    if qty <= 0:
                        continue

                    spreads.append(
                        CreditSpread(
                            underlying=symbol,
                            expiry_code=expiry_code,
                            expiry_date=short_leg.expiry_date,
                            spread_type="BULL_PUT",
                            short_leg=short_leg,
                            long_leg=long_leg,
                            quantity=qty,
                        )
                    )

                    remaining_short[short_leg.tradingsymbol] -= qty
                    remaining_long[long_leg.tradingsymbol] -= qty

                    if remaining_short[short_leg.tradingsymbol] <= 0:
                        break

    return spreads


def credit_spread_short_qty_map(
    positions: List[dict],
    symbol: str,
) -> Dict[str, int]:
    out: Dict[str, int] = {}

    for spread in detect_credit_spreads(positions, symbol):
        out[spread.short_leg.tradingsymbol] = (
            out.get(spread.short_leg.tradingsymbol, 0) + spread.quantity
        )

    return out


def detect_short_strangles(
    positions: List[dict],
    symbol: str,
) -> List[ShortStrangle]:
    symbol = symbol.upper().strip()
    spread_short_qty = credit_spread_short_qty_map(positions, symbol)
    covered_put_short_qty = {
        cp.leg.tradingsymbol: cp.quantity
        for cp in detect_covered_puts(positions, symbol)
    }

    short_pes: Dict[str, List[OptionLeg]] = {}
    short_ces: Dict[str, List[OptionLeg]] = {}

    for p in positions:
        qty = int(p.get("quantity") or 0)
        if qty >= 0:
            continue

        ts = p.get("tradingsymbol", "")
        parsed = parse_option_symbol(ts)
        if not parsed:
            continue

        underlying, expiry_code, expiry_dt, strike, option_type = parsed
        if underlying != symbol:
            continue

        leg = OptionLeg(
            tradingsymbol=ts,
            underlying=underlying,
            expiry_code=expiry_code,
            expiry_date=expiry_dt,
            strike=strike,
            option_type=option_type,
            quantity=qty,
            average_price=float(p.get("average_price") or 0.0),
            last_price=float(p.get("last_price") or 0.0),
        )

        if option_type == "PE":
            short_pes.setdefault(expiry_code, []).append(leg)
        elif option_type == "CE":
            short_ces.setdefault(expiry_code, []).append(leg)

    strangles: List[ShortStrangle] = []

    for expiry_code in sorted(set(short_pes.keys()) & set(short_ces.keys())):
        pes = sorted(short_pes[expiry_code], key=lambda x: x.strike)
        ces = sorted(short_ces[expiry_code], key=lambda x: x.strike)

        if not pes or not ces:
            continue

        # Production rule for first version:
        # choose widest short-strangle core = lowest short PE + highest short CE.
        pe = pes[0]
        ce = ces[-1]

        if pe.strike >= ce.strike:
            continue

        qty = min(abs(pe.quantity), abs(ce.quantity))

        strangles.append(
            ShortStrangle(
                underlying=symbol,
                expiry_code=expiry_code,
                expiry_date=pe.expiry_date,
                short_put=pe,
                short_call=ce,
                quantity=qty,
            )
        )

    return strangles




def long_future_qty_for(
    positions: List[dict],
    symbol: str,
    expiry_code: str,
) -> int:
    expected = f"{symbol}{expiry_code}FUT"
    qty = 0

    for p in positions:
        if p.get("tradingsymbol") == expected:
            qty += max(0, int(p.get("quantity") or 0))

    return qty


def short_future_qty_for(
    positions: List[dict],
    symbol: str,
    expiry_code: str,
) -> int:
    expected = f"{symbol}{expiry_code}FUT"
    qty = 0

    for p in positions:
        if p.get("tradingsymbol") == expected:
            qty += abs(min(0, int(p.get("quantity") or 0)))

    return qty


def holding_qty_for(
    holdings: List[dict],
    symbol: str,
) -> int:
    qty = 0

    for h in holdings:
        if str(h.get("tradingsymbol", "")).upper() == symbol.upper():
            qty += max(0, int(h.get("quantity") or 0))

    return qty



def detect_covered_calls(
    positions: List[dict],
    symbol: str,
    holdings: Optional[List[dict]] = None,
) -> List[CoveredCall]:
    """
    Detect short CE legs covered by long same-expiry futures or stock holdings.
    """
    symbol = symbol.upper().strip()
    holdings = holdings or []

    covered_calls: List[CoveredCall] = []
    stock_cover_available = holding_qty_for(holdings, symbol)

    short_ces: Dict[str, List[OptionLeg]] = {}

    for p in positions:
        qty = int(p.get("quantity") or 0)
        if qty >= 0:
            continue

        ts = p.get("tradingsymbol", "")
        parsed = parse_option_symbol(ts)
        if not parsed:
            continue

        underlying, expiry_code, expiry_dt, strike, option_type = parsed
        if underlying != symbol or option_type != "CE":
            continue

        leg = OptionLeg(
            tradingsymbol=ts,
            underlying=underlying,
            expiry_code=expiry_code,
            expiry_date=expiry_dt,
            strike=strike,
            option_type=option_type,
            quantity=qty,
            average_price=float(p.get("average_price") or 0.0),
            last_price=float(p.get("last_price") or 0.0),
        )

        short_ces.setdefault(expiry_code, []).append(leg)

    for expiry_code, ces in short_ces.items():
        future_cover_available = long_future_qty_for(positions, symbol, expiry_code)

        for leg in sorted(ces, key=lambda x: x.strike):
            short_qty = abs(leg.quantity)

            fut_cover = min(short_qty, future_cover_available)
            future_cover_available -= fut_cover

            if fut_cover > 0:
                covered_calls.append(
                    CoveredCall(
                        underlying=symbol,
                        expiry_code=leg.expiry_code,
                        expiry_date=leg.expiry_date,
                        leg=leg,
                        quantity=fut_cover,
                        cover_type="FUTURE",
                    )
                )

            remaining = short_qty - fut_cover

            stock_cover = min(remaining, stock_cover_available)
            stock_cover_available -= stock_cover

            if stock_cover > 0:
                covered_calls.append(
                    CoveredCall(
                        underlying=symbol,
                        expiry_code=leg.expiry_code,
                        expiry_date=leg.expiry_date,
                        leg=leg,
                        quantity=stock_cover,
                        cover_type="STOCK",
                    )
                )

    return covered_calls


def detect_covered_puts(
    positions: List[dict],
    symbol: str,
) -> List[CoveredPut]:
    """
    Detect short PE legs covered by short same-expiry futures.

    Covered put:
    - Short FUT
    - Short PE
    """
    symbol = symbol.upper().strip()

    covered_puts: List[CoveredPut] = []
    short_pes: Dict[str, List[OptionLeg]] = {}

    for p in positions:
        qty = int(p.get("quantity") or 0)
        if qty >= 0:
            continue

        ts = p.get("tradingsymbol", "")
        parsed = parse_option_symbol(ts)
        if not parsed:
            continue

        underlying, expiry_code, expiry_dt, strike, option_type = parsed
        if underlying != symbol or option_type != "PE":
            continue

        leg = OptionLeg(
            tradingsymbol=ts,
            underlying=underlying,
            expiry_code=expiry_code,
            expiry_date=expiry_dt,
            strike=strike,
            option_type=option_type,
            quantity=qty,
            average_price=float(p.get("average_price") or 0.0),
            last_price=float(p.get("last_price") or 0.0),
        )

        short_pes.setdefault(expiry_code, []).append(leg)

    for expiry_code, pes in short_pes.items():
        future_cover_available = short_future_qty_for(positions, symbol, expiry_code)

        # For covered puts, nearest higher-risk short put should consume cover first.
        for leg in sorted(pes, key=lambda x: x.strike, reverse=True):
            short_qty = abs(leg.quantity)
            cover_qty = min(short_qty, future_cover_available)
            future_cover_available -= cover_qty

            if cover_qty <= 0:
                continue

            covered_puts.append(
                CoveredPut(
                    underlying=symbol,
                    expiry_code=leg.expiry_code,
                    expiry_date=leg.expiry_date,
                    leg=leg,
                    quantity=cover_qty,
                    cover_type="FUTURE",
                )
            )

    return covered_puts


def evaluate_covered_put_risk(
    distance_pct: float,
    net_delta: float,
    pnl_unit: float,
    entry_premium_unit: float,
) -> Tuple[str, List[str]]:
    """
    Covered put risk:
    - upside loss from short future
    - downside profit capped by short put
    - pressure increases when spot is near/below short PE strike
    """
    loss_unit = max(0.0, -pnl_unit)
    abs_delta = abs(net_delta)

    if (
        distance_pct < 0.01
        or abs_delta > 0.65
        or loss_unit > (2.0 * entry_premium_unit)
    ):
        return "ADJUST", ["COVERED_PUT_NEAR_STRIKE_OR_HIGH_DELTA"]

    if (
        distance_pct < 0.03
        or abs_delta > 0.45
        or loss_unit > (1.0 * entry_premium_unit)
    ):
        return "WATCH", ["COVERED_PUT_PRESSURE_BUILDING"]

    return "HOLD", ["COVERED_PUT_WITHIN_LIMITS"]


def evaluate_covered_call_risk(
    distance_pct: float,
    short_delta: float,
    pnl_unit: float,
    entry_premium_unit: float,
) -> Tuple[str, List[str]]:
    """
    Covered call risk is different from naked call risk.

    Main risk:
    - upside opportunity loss
    - assignment / settlement pressure near strike
    - hedge effectiveness if covered by futures
    """
    loss_unit = max(0.0, -pnl_unit)
    abs_delta = abs(short_delta)

    if (
        distance_pct < 0.01
        or abs_delta > 0.55
        or loss_unit > (2.0 * entry_premium_unit)
    ):
        return "ADJUST", ["COVERED_CALL_NEAR_STRIKE_OR_HIGH_DELTA"]

    if (
        distance_pct < 0.03
        or abs_delta > 0.35
        or loss_unit > (1.0 * entry_premium_unit)
    ):
        return "WATCH", ["COVERED_CALL_PRESSURE_BUILDING"]

    return "HOLD", ["COVERED_CALL_WITHIN_LIMITS"]


def detect_naked_options(
    positions: List[dict],
    symbol: str,
    holdings: Optional[List[dict]] = None,
) -> List[NakedOption]:
    """
    Detect standalone short option legs that are not part of a short strangle.

    Covered-call rule:
    - Remaining short CE is NOT treated as naked if covered by long FUT or stock holding.
    - Covered calls are intentionally excluded from naked risk logic.
    """
    symbol = symbol.upper().strip()
    holdings = holdings or []
    spread_short_qty = credit_spread_short_qty_map(positions, symbol)
    covered_put_short_qty = {
        cp.leg.tradingsymbol: cp.quantity
        for cp in detect_covered_puts(positions, symbol)
    }

    short_pes: Dict[str, List[OptionLeg]] = {}
    short_ces: Dict[str, List[OptionLeg]] = {}

    for p in positions:
        qty = int(p.get("quantity") or 0)
        if qty >= 0:
            continue

        ts = p.get("tradingsymbol", "")
        qty += min(abs(qty), spread_short_qty.get(ts, 0))
        qty += min(abs(qty), covered_put_short_qty.get(ts, 0))
        if qty >= 0:
            continue

        parsed = parse_option_symbol(ts)
        if not parsed:
            continue

        underlying, expiry_code, expiry_dt, strike, option_type = parsed
        if underlying != symbol:
            continue

        leg = OptionLeg(
            tradingsymbol=ts,
            underlying=underlying,
            expiry_code=expiry_code,
            expiry_date=expiry_dt,
            strike=strike,
            option_type=option_type,
            quantity=qty,
            average_price=float(p.get("average_price") or 0.0),
            last_price=float(p.get("last_price") or 0.0),
        )

        if option_type == "PE":
            short_pes.setdefault(expiry_code, []).append(leg)
        elif option_type == "CE":
            short_ces.setdefault(expiry_code, []).append(leg)

    naked: List[NakedOption] = []

    stock_cover_available = holding_qty_for(holdings, symbol)

    for expiry_code in sorted(set(short_pes.keys()) | set(short_ces.keys())):
        pes = sorted(short_pes.get(expiry_code, []), key=lambda x: x.strike)
        ces = sorted(short_ces.get(expiry_code, []), key=lambda x: x.strike)

        paired_pe = pes[0] if pes and ces and pes[0].strike < ces[-1].strike else None
        paired_ce = ces[-1] if pes and ces and pes[0].strike < ces[-1].strike else None

        future_cover_available = long_future_qty_for(positions, symbol, expiry_code)

        for leg in pes + ces:
            if paired_pe and leg.tradingsymbol == paired_pe.tradingsymbol:
                continue
            if paired_ce and leg.tradingsymbol == paired_ce.tradingsymbol:
                continue

            short_qty = abs(leg.quantity)

            if leg.option_type == "CE":
                covered_by_future = min(short_qty, future_cover_available)
                future_cover_available -= covered_by_future
                remaining_after_future = short_qty - covered_by_future

                covered_by_stock = min(remaining_after_future, stock_cover_available)
                stock_cover_available -= covered_by_stock
                remaining_uncovered = remaining_after_future - covered_by_stock

                if remaining_uncovered <= 0:
                    continue

                naked_qty = remaining_uncovered
            else:
                naked_qty = short_qty

            naked.append(
                NakedOption(
                    underlying=symbol,
                    expiry_code=leg.expiry_code,
                    expiry_date=leg.expiry_date,
                    leg=leg,
                    quantity=naked_qty,
                )
            )

    return naked

def choose_nearest_future_symbol(positions: List[dict], symbol: str, expiry_code: str) -> Optional[str]:
    """
    Prefer same-expiry future if present in positions.
    Otherwise return constructed symbol e.g. TRENT26MAYFUT.
    """
    expected = f"{symbol}{expiry_code}FUT"

    for p in positions:
        if p.get("tradingsymbol") == expected:
            return expected

    return expected


# ============================================================
# Market data
# ============================================================


INDEX_SPOT_INSTRUMENTS = {
    "NIFTY": "NSE:NIFTY 50",
    "BANKNIFTY": "NSE:NIFTY BANK",
    "FINNIFTY": "NSE:NIFTY FIN SERVICE",
    "MIDCPNIFTY": "NSE:NIFTY MID SELECT",
    "SENSEX": "BSE:SENSEX",
}


def spot_instrument_for(symbol: str) -> str:
    symbol = symbol.upper().strip()
    return INDEX_SPOT_INSTRUMENTS.get(symbol, f"NSE:{symbol}")


def get_ltp_map(kite: KiteConnect, instruments: List[str]) -> Dict[str, float]:
    data = kite.ltp(instruments)
    out: Dict[str, float] = {}

    for ins in instruments:
        if ins not in data:
            raise RuntimeError(f"LTP not returned for instrument={ins}")
        out[ins] = float(data[ins]["last_price"])

    return out


# ============================================================
# Black-Scholes forward model
# ============================================================

def year_fraction_to_market_close(expiry_dt: date) -> float:
    now = datetime.now()
    expiry_datetime = datetime.combine(expiry_dt, dtime(hour=15, minute=30))
    seconds = max((expiry_datetime - now).total_seconds(), 1)
    return seconds / (365.0 * 24.0 * 60.0 * 60.0)


def bs_forward_price(
    future: float,
    strike: float,
    t: float,
    r: float,
    sigma: float,
    option_type: str,
) -> float:
    if t <= 0 or sigma <= 0:
        if option_type == "CE":
            return max(0.0, future - strike)
        return max(0.0, strike - future)

    d1 = (math.log(future / strike) + 0.5 * sigma * sigma * t) / (sigma * math.sqrt(t))
    d2 = d1 - sigma * math.sqrt(t)
    discount = math.exp(-r * t)

    if option_type == "CE":
        return discount * (future * norm.cdf(d1) - strike * norm.cdf(d2))

    return discount * (strike * norm.cdf(-d2) - future * norm.cdf(-d1))


def implied_volatility(
    market_price: float,
    future: float,
    strike: float,
    t: float,
    r: float,
    option_type: str,
) -> float:
    if market_price <= 0:
        raise RuntimeError(f"Invalid market price for IV: {market_price}")

    def objective(sigma: float) -> float:
        return bs_forward_price(future, strike, t, r, sigma, option_type) - market_price

    try:
        return brentq(objective, 0.0001, 5.0, maxiter=100)
    except ValueError as exc:
        raise RuntimeError(
            f"Unable to solve IV | price={market_price} future={future} "
            f"strike={strike} type={option_type}"
        ) from exc


def greeks_forward(
    future: float,
    strike: float,
    t: float,
    r: float,
    sigma: float,
    option_type: str,
) -> LegGreeks:
    if t <= 0 or sigma <= 0:
        raise RuntimeError("Invalid time/sigma for Greeks")

    d1 = (math.log(future / strike) + 0.5 * sigma * sigma * t) / (sigma * math.sqrt(t))
    d2 = d1 - sigma * math.sqrt(t)
    discount = math.exp(-r * t)

    if option_type == "CE":
        delta = discount * norm.cdf(d1)
        theta = (
            -discount * future * norm.pdf(d1) * sigma / (2.0 * math.sqrt(t))
            - r * discount * (future * norm.cdf(d1) - strike * norm.cdf(d2))
        ) / 365.0
    else:
        delta = -discount * norm.cdf(-d1)
        theta = (
            -discount * future * norm.pdf(d1) * sigma / (2.0 * math.sqrt(t))
            - r * discount * (strike * norm.cdf(-d2) - future * norm.cdf(-d1))
        ) / 365.0

    gamma = discount * norm.pdf(d1) / (future * sigma * math.sqrt(t))
    vega = discount * future * norm.pdf(d1) * math.sqrt(t) / 100.0

    return LegGreeks(
        iv_pct=sigma * 100.0,
        delta=delta,
        gamma=gamma,
        theta=theta,
        vega=vega,
    )


# ============================================================
# Risk evaluation
# ============================================================

def evaluate_risk(
    snapshot: StrategySnapshot,
) -> Tuple[str, List[str]]:
    reasons: List[str] = []

    loss_unit = max(0.0, -snapshot.pnl_unit)

    # EXIT
    # Hard exit only when the trade is genuinely broken.
    # Do not use risk_score alone as an exit trigger because it is an acceleration-warning metric.
    if (
        abs(snapshot.net_delta) > 0.35
        or snapshot.dist_put_pct < 0.005
        or snapshot.dist_call_pct < 0.005
        or snapshot.iv_change_points > 5.0
        or loss_unit > (2.0 * snapshot.entry_premium_unit)
    ):
        return "EXIT", ["EXTREME_RISK_EXIT"]

    # ADJUST
    # risk_score belongs here: it warns that gamma risk is accelerating near a short strike.
    if (
        snapshot.risk_score > 0.30
        or abs(snapshot.net_delta) > 0.25
        or snapshot.dist_put_pct < 0.015
        or snapshot.dist_call_pct < 0.015
        or snapshot.iv_change_points > 3.0
        or loss_unit > (1.50 * snapshot.entry_premium_unit)
    ):
        return "ADJUST", ["MODERATE_RISK_ADJUST"]

    # WATCH
    if (
        snapshot.risk_score > 0.05
        or abs(snapshot.net_delta) > 0.15
        or snapshot.dist_put_pct < 0.03
        or snapshot.dist_call_pct < 0.03
        or snapshot.iv_change_points > 2.0
        or loss_unit > (1.25 * snapshot.entry_premium_unit)
    ):
        return "WATCH", ["EARLY_WARNING_WATCH"]

    return "HOLD", ["RISK_WITHIN_LIMITS"]




def evaluate_naked_risk(
    option_type: str,
    signed_short_delta: float,
    distance_pct: float,
    iv_change_points: float,
    pnl_unit: float,
    entry_premium_unit: float,
) -> Tuple[str, List[str]]:
    """
    Separate risk logic for naked short options.

    Naked options are one-sided risk positions, so thresholds are stricter
    than short strangle thresholds.
    """
    loss_unit = max(0.0, -pnl_unit)
    abs_delta = abs(signed_short_delta)

    if (
        abs_delta > 0.30
        or distance_pct < 0.03
        or iv_change_points > 3.0
        or loss_unit > (1.0 * entry_premium_unit)
    ):
        return "EXIT", [f"NAKED_{option_type}_EXTREME_RISK_EXIT"]

    if (
        abs_delta > 0.20
        or distance_pct < 0.05
        or iv_change_points > 2.0
        or loss_unit > (0.50 * entry_premium_unit)
    ):
        return "ADJUST", [f"NAKED_{option_type}_MODERATE_RISK_ADJUST"]

    return "WATCH", [f"NAKED_{option_type}_MONITOR_CLOSELY"]


def monitor_naked_option_once(
    kite: KiteConnect,
    positions: List[dict],
    user_id: str,
    naked: NakedOption,
    risk_free_rate: float,
) -> None:
    symbol = naked.underlying
    leg = naked.leg

    spot_instrument = spot_instrument_for(symbol)
    future_symbol = choose_nearest_future_symbol(positions, symbol, leg.expiry_code)
    future_instrument = f"NFO:{future_symbol}"
    option_instrument = f"NFO:{leg.tradingsymbol}"

    ltp = get_ltp_map(kite, [spot_instrument, future_instrument, option_instrument])

    spot = ltp[spot_instrument]
    future = ltp[future_instrument]
    option_ltp = ltp[option_instrument]

    t = year_fraction_to_market_close(leg.expiry_date)

    iv = implied_volatility(
        option_ltp,
        future,
        leg.strike,
        t,
        risk_free_rate,
        leg.option_type,
    )

    greeks = greeks_forward(
        future,
        leg.strike,
        t,
        risk_free_rate,
        iv,
        leg.option_type,
    )

    # Short option delta = negative of long option delta.
    signed_short_delta = -greeks.delta

    if leg.option_type == "CE":
        distance_pct = (leg.strike - spot) / spot
    else:
        distance_pct = (spot - leg.strike) / spot

    entry_premium_unit = float(leg.average_price or 0.0)
    current_premium_unit = option_ltp
    pnl_unit = entry_premium_unit - current_premium_unit
    pnl_scaled = pnl_unit * naked.quantity

    decision, reasons = evaluate_naked_risk(
        option_type=leg.option_type,
        signed_short_delta=signed_short_delta,
        distance_pct=distance_pct,
        iv_change_points=0.0,
        pnl_unit=pnl_unit,
        entry_premium_unit=entry_premium_unit,
    )

    print("\n" + "=" * 104)
    print(f"{symbol} | NAKED SHORT {leg.option_type} RISK DASHBOARD | USER {user_id}")
    print("=" * 104)
    print(f"Time                : {datetime.now().isoformat(timespec='seconds')}")
    print(f"Decision            : {decision}")
    print(f"Reasons             : {', '.join(reasons)}")
    print("-" * 104)
    print("STRUCTURE")
    print(f"Short Option        : SELL {leg.tradingsymbol} | Strike {leg.strike}")
    print(f"Quantity            : {naked.quantity}")
    print("-" * 104)
    print("MARKET")
    print(f"Spot                : {spot:.2f}")
    print(f"Future              : {future:.2f}")
    print(f"Option LTP          : {option_ltp:.2f}")
    print("-" * 104)
    print("RISK")
    print(f"Short Delta         : {signed_short_delta:+.4f} | Exit limit +/-0.30")
    print(f"Gamma               : {-greeks.gamma:+.6f}")
    print(f"Theta               : {-greeks.theta:+.4f}")
    print(f"Vega                : {-greeks.vega:+.4f}")
    print(f"IV                  : {greeks.iv_pct:.2f}%")
    print(f"Distance            : {distance_pct * 100:.2f}% | Naked exit threshold 3.00%")
    print("-" * 104)
    print("PREMIUM / P&L")
    print(f"Entry Premium       : {entry_premium_unit:.2f}")
    print(f"Current Premium     : {current_premium_unit:.2f}")
    print(f"PnL / Unit          : {pnl_unit:+.2f}")
    print(f"PnL / Position      : {pnl_scaled:+.2f}")
    print("=" * 104)


# ============================================================
# Snapshot building
# ============================================================

def build_snapshot(
    kite: KiteConnect,
    positions: List[dict],
    user_id: str,
    strangle: ShortStrangle,
    baseline_iv: Optional[Dict[str, float]],
    risk_free_rate: float,
) -> Tuple[StrategySnapshot, Dict[str, float]]:
    symbol = strangle.underlying

    spot_instrument = spot_instrument_for(symbol)
    future_symbol = choose_nearest_future_symbol(positions, symbol, strangle.expiry_code)
    future_instrument = f"NFO:{future_symbol}"

    put_instrument = f"NFO:{strangle.short_put.tradingsymbol}"
    call_instrument = f"NFO:{strangle.short_call.tradingsymbol}"

    ltp = get_ltp_map(
        kite,
        [
            spot_instrument,
            future_instrument,
            put_instrument,
            call_instrument,
        ],
    )

    spot = ltp[spot_instrument]
    future = ltp[future_instrument]
    put_ltp = ltp[put_instrument]
    call_ltp = ltp[call_instrument]

    t = year_fraction_to_market_close(strangle.expiry_date)

    put_iv = implied_volatility(
        put_ltp,
        future,
        strangle.short_put.strike,
        t,
        risk_free_rate,
        "PE",
    )
    call_iv = implied_volatility(
        call_ltp,
        future,
        strangle.short_call.strike,
        t,
        risk_free_rate,
        "CE",
    )

    put_g = greeks_forward(
        future,
        strangle.short_put.strike,
        t,
        risk_free_rate,
        put_iv,
        "PE",
    )
    call_g = greeks_forward(
        future,
        strangle.short_call.strike,
        t,
        risk_free_rate,
        call_iv,
        "CE",
    )

    # Since both are SELL legs, multiply long-option Greeks by -1.
    net_delta = -(put_g.delta + call_g.delta)
    net_gamma = -(put_g.gamma + call_g.gamma)
    net_theta = -(put_g.theta + call_g.theta)
    net_vega = -(put_g.vega + call_g.vega)

    entry_premium_unit = (
        float(strangle.short_put.average_price or 0.0)
        + float(strangle.short_call.average_price or 0.0)
    )

    current_premium_unit = put_ltp + call_ltp

    # Short premium position:
    # Profit = entry premium - current buyback premium.
    pnl_unit = entry_premium_unit - current_premium_unit
    pnl_scaled = pnl_unit * strangle.quantity

    if baseline_iv is None:
        baseline_iv = {
            "put_iv_pct": put_g.iv_pct,
            "call_iv_pct": call_g.iv_pct,
        }

    iv_change_points = max(
        put_g.iv_pct - baseline_iv["put_iv_pct"],
        call_g.iv_pct - baseline_iv["call_iv_pct"],
    )

    dist_put_pct = (spot - strangle.short_put.strike) / spot
    dist_call_pct = (strangle.short_call.strike - spot) / spot

    min_dist = max(min(dist_put_pct, dist_call_pct), 0.0001)
    risk_score = abs(net_gamma) / min_dist

    snapshot = StrategySnapshot(
        risk_score=risk_score,
        timestamp=datetime.now().isoformat(timespec="seconds"),
        user_id=user_id,
        symbol=symbol,
        spot=spot,
        future=future,
        short_put_symbol=strangle.short_put.tradingsymbol,
        short_call_symbol=strangle.short_call.tradingsymbol,
        short_put_strike=strangle.short_put.strike,
        short_call_strike=strangle.short_call.strike,
        short_put_ltp=put_ltp,
        short_call_ltp=call_ltp,
        short_put_iv=put_g.iv_pct,
        short_call_iv=call_g.iv_pct,
        net_delta=net_delta,
        net_gamma=net_gamma,
        net_theta=net_theta,
        net_vega=net_vega,
        dist_put_pct=dist_put_pct,
        dist_call_pct=dist_call_pct,
        entry_premium_unit=entry_premium_unit,
        current_premium_unit=current_premium_unit,
        pnl_unit=pnl_unit,
        pnl_scaled=pnl_scaled,
        iv_change_points=iv_change_points,
        delta_change=0.0,
        gamma_change=0.0,
        pnl_change=0.0,
        decision="HOLD",
        reason_codes=[],
    )

    decision, reasons = evaluate_risk(snapshot)
    snapshot.decision = decision
    snapshot.reason_codes = reasons

    return snapshot, baseline_iv



def is_data_sane(snapshot: StrategySnapshot) -> bool:
    # IV sanity
    if snapshot.short_put_iv > 150 or snapshot.short_call_iv > 150:
        return False
    if snapshot.short_put_iv < 5 or snapshot.short_call_iv < 5:
        return False

    # distance sanity
    if snapshot.dist_put_pct < -0.2 or snapshot.dist_call_pct < -0.2:
        return False

    # extreme IV jump
    if abs(snapshot.iv_change_points) > 20:
        return False

    return True


# ============================================================
# Reporting
# ============================================================

def risk_state_for(snapshot: StrategySnapshot) -> str:
    if snapshot.decision == "EXIT":
        return "DANGER"
    if snapshot.decision == "ADJUST":
        return "HIGH"
    if snapshot.decision == "WATCH":
        return "WATCH"
    return "LOW"


def nearest_risk_side(snapshot: StrategySnapshot) -> str:
    if snapshot.dist_put_pct <= snapshot.dist_call_pct:
        return f"PUT side ({snapshot.dist_put_pct * 100:.2f}%)"
    return f"CALL side ({snapshot.dist_call_pct * 100:.2f}%)"


def format_signed(value: float, decimals: int = 4) -> str:
    return f"{value:+.{decimals}f}"


def print_snapshot(snapshot: StrategySnapshot) -> None:
    risk_state = risk_state_for(snapshot)
    nearest_side = nearest_risk_side(snapshot)

    print("\n" + "=" * 104)
    print(f"{snapshot.symbol} | SHORT STRANGLE RISK DASHBOARD | USER {snapshot.user_id}")
    print("=" * 104)
    print(f"Time                : {snapshot.timestamp}")
    print(f"Risk State          : {risk_state}")
    print(f"Risk | Decision            : {snapshot.decision}")
    print(f"Reasons             : {', '.join(snapshot.reason_codes)}")

    print("-" * 104)
    print("STRUCTURE")
    print(f"Short Put           : SELL {snapshot.short_put_symbol} | Strike {snapshot.short_put_strike}")
    print(f"Short Call          : SELL {snapshot.short_call_symbol} | Strike {snapshot.short_call_strike}")

    print("-" * 104)
    print("MARKET")
    print(f"Spot                : {snapshot.spot:.2f}")
    print(f"Future              : {snapshot.future:.2f}")
    print(f"Put LTP             : {snapshot.short_put_ltp:.2f}")
    print(f"Call LTP            : {snapshot.short_call_ltp:.2f}")

    print("-" * 104)
    print("GREEKS")
    print(f"Delta               : {snapshot.net_delta:+.4f} | Change {format_signed(snapshot.delta_change, 4)} | Exit limit +/-{DELTA_EXIT:.2f}")
    print(f"Gamma               : {snapshot.net_gamma:+.6f} | Change {format_signed(snapshot.gamma_change, 6)}")
    print(f"Theta               : {snapshot.net_theta:+.4f}")
    print(f"Vega                : {snapshot.net_vega:+.4f}")

    print("-" * 104)
    print("VOLATILITY")
    print(f"Put IV              : {snapshot.short_put_iv:.2f}%")
    print(f"Call IV             : {snapshot.short_call_iv:.2f}%")
    print(f"IV Change           : {snapshot.iv_change_points:+.2f} pts | Watch {IV_WATCH_POINTS:.2f} | Exit {IV_EXIT_POINTS:.2f}")

    print("-" * 104)
    print("DISTANCE TO SHORT STRIKES")
    print(f"Put Distance        : {snapshot.dist_put_pct * 100:.2f}% | Exit {DISTANCE_EXIT_PCT * 100:.2f}%")
    print(f"Call Distance       : {snapshot.dist_call_pct * 100:.2f}% | Exit {DISTANCE_EXIT_PCT * 100:.2f}%")
    print(f"Nearest Risk Side   : {nearest_side}")

    print("-" * 104)
    print("PREMIUM / P&L")
    print(f"Entry Premium       : {snapshot.entry_premium_unit:.2f}")
    print(f"Current Premium     : {snapshot.current_premium_unit:.2f}")
    print(f"PnL / Unit          : {snapshot.pnl_unit:+.2f} | Change {format_signed(snapshot.pnl_change, 2)}")
    print(f"PnL / Position      : {snapshot.pnl_scaled:+.2f}")
    print(f"Hard Loss Exit      : -{snapshot.entry_premium_unit * LOSS_EXIT_MULTIPLE:.2f} per unit")
    print("=" * 104)


def ensure_log_file(log_path: Path) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    if log_path.exists():
        return

    with log_path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "timestamp",
            "user_id",
            "symbol",
            "spot",
            "future",
            "short_put",
            "short_call",
            "put_ltp",
            "call_ltp",
            "put_iv",
            "call_iv",
            "net_delta",
            "net_gamma",
            "net_theta",
            "net_vega",
            "risk_score",
            "dist_put_pct",
            "dist_call_pct",
            "entry_premium_unit",
            "current_premium_unit",
            "pnl_unit",
            "pnl_scaled",
            "iv_change_points",
            "delta_change",
            "gamma_change",
            "pnl_change",
            "decision",
            "reason_codes",
        ])


def append_snapshot(log_path: Path, snapshot: StrategySnapshot) -> None:
    ensure_log_file(log_path)

    with log_path.open("a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            snapshot.timestamp,
            snapshot.user_id,
            snapshot.symbol,
            snapshot.spot,
            snapshot.future,
            snapshot.short_put_symbol,
            snapshot.short_call_symbol,
            snapshot.short_put_ltp,
            snapshot.short_call_ltp,
            snapshot.short_put_iv,
            snapshot.short_call_iv,
            snapshot.net_delta,
            snapshot.net_gamma,
            snapshot.net_theta,
            snapshot.net_vega,
            snapshot.risk_score,
            snapshot.dist_put_pct,
            snapshot.dist_call_pct,
            snapshot.entry_premium_unit,
            snapshot.current_premium_unit,
            snapshot.pnl_unit,
            snapshot.pnl_scaled,
            snapshot.iv_change_points,
            snapshot.delta_change,
            snapshot.gamma_change,
            snapshot.pnl_change,
            snapshot.decision,
            "|".join(snapshot.reason_codes),
        ])



def read_previous_snapshot(log_path: Path) -> Optional[dict]:
    if not log_path.exists():
        return None

    with log_path.open("r", newline="") as f:
        rows = list(csv.DictReader(f))

    if not rows:
        return None

    return rows[-1]


def safe_float(row: dict, key: str) -> Optional[float]:
    try:
        value = row.get(key)
        if value in (None, ""):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def apply_snapshot_changes(snapshot: StrategySnapshot, previous: Optional[dict]) -> None:
    if not previous:
        snapshot.delta_change = 0.0
        snapshot.gamma_change = 0.0
        snapshot.pnl_change = 0.0
        return

    previous_delta = safe_float(previous, "net_delta")
    previous_gamma = safe_float(previous, "net_gamma")
    previous_pnl = safe_float(previous, "pnl_unit")

    snapshot.delta_change = 0.0 if previous_delta is None else snapshot.net_delta - previous_delta
    snapshot.gamma_change = 0.0 if previous_gamma is None else snapshot.net_gamma - previous_gamma
    snapshot.pnl_change = 0.0 if previous_pnl is None else snapshot.pnl_unit - previous_pnl



def print_history(log_path: Path, last_n: int = 5) -> None:
    try:
        with log_path.open("r", newline="") as f:
            rows = list(csv.DictReader(f))
    except FileNotFoundError:
        print("\nHISTORY: No log file yet")
        return

    if not rows:
        print("\nHISTORY: Empty")
        return

    def safe_float(row, key, default=0.0):
        try:
            return float(row.get(key, default) or default)
        except Exception:
            return default

    def safe_decision(row):
        decision = str(row.get("decision", "")).strip()
        return decision if decision in ("HOLD", "WATCH", "ADJUST", "EXIT") else "UNKNOWN"

    def arrow(value):
        if value > 0.001:
            return "UP"
        if value < -0.001:
            return "DN"
        return "--"

    print("\nHISTORY (last {} snapshots)".format(last_n))
    print("-" * 132)
    print("Time     | Spot    | Delta   | ΔTrend | IVChg | MinDist | PnL/unit | PnLΔ   | RiskSide | Risk | Decision")
    print("-" * 132)

    for r in rows[-last_n:]:
        timestamp = r.get("timestamp", "")
        time_part = timestamp[-8:] if len(timestamp) >= 8 else timestamp

        spot = safe_float(r, "spot")
        delta = safe_float(r, "net_delta")
        delta_change = safe_float(r, "delta_change")
        iv_change = safe_float(r, "iv_change_points")
        pnl_unit = safe_float(r, "pnl_unit")
        pnl_change = safe_float(r, "pnl_change")

        put_dist = safe_float(r, "dist_put_pct") * 100
        call_dist = safe_float(r, "dist_call_pct") * 100
        min_dist = min(put_dist, call_dist)

        if put_dist < call_dist:
            risk_side = "PUT"
        elif call_dist < put_dist:
            risk_side = "CALL"
        else:
            risk_side = "NONE"

        print(
            f"{time_part:8} | "
            f"{spot:7.2f} | "
            f"{delta:+.4f} | "
            f"{arrow(delta_change):6} | "
            f"{iv_change:+.2f} | "
            f"{min_dist:6.2f}% | "
            f"{pnl_unit:+8.2f} | "
            f"{pnl_change:+6.2f} | "
            f"{risk_side:8} | "
            f"{safe_float(r, 'risk_score'):.3f} | {safe_decision(r)}"
        )

    print("-" * 132)




def print_last_snapshot_from_log(log_path: Path) -> None:
    if not log_path.exists():
        print("No previous data available.")
        return

    with log_path.open("r") as f:
        rows = list(csv.DictReader(f))

    if not rows:
        print("Log file empty.")
        return

    last = rows[-1]

    print("\n" + "=" * 104)
    print("LAST KNOWN SNAPSHOT (MARKET CLOSED)")
    print("=" * 104)

    print(f"Time                : {last.get('timestamp')}")
    print(f"Spot                : {float(last.get('spot', 0)):.2f}")
    print(f"PnL / Unit          : {float(last.get('pnl_unit', 0)):+.2f}")
    print(f"Decision            : {last.get('decision')}")
    print("=" * 104)

    print_history(log_path, last_n=5)




def risk_zone_from_distance(distance_pct: float) -> str:
    if distance_pct < 0:
        return "ITM"
    if distance_pct < 1.0:
        return "DANGER"
    if distance_pct < 3.0:
        return "NEAR"
    if distance_pct < 6.0:
        return "WATCH"
    return "SAFE"


def expiry_pnl_at_spot_for_short_option(
    option_type: str,
    strike: float,
    spot: float,
    premium: float,
    quantity: int,
) -> float:
    """
    Expiry PnL for a short option if expiry happens at the current spot.
    """
    if option_type == "PE":
        intrinsic = max(strike - spot, 0.0)
    elif option_type == "CE":
        intrinsic = max(spot - strike, 0.0)
    else:
        intrinsic = 0.0

    return (premium - intrinsic) * quantity


def expiry_pnl_at_spot_for_long_option(
    option_type: str,
    strike: float,
    spot: float,
    premium: float,
    quantity: int,
) -> float:
    """
    Expiry PnL for a long option if expiry happens at the current spot.
    """
    if option_type == "PE":
        intrinsic = max(strike - spot, 0.0)
    elif option_type == "CE":
        intrinsic = max(spot - strike, 0.0)
    else:
        intrinsic = 0.0

    return (intrinsic - premium) * quantity


def evaluate_credit_spread_risk(
    spread_type: str,
    distance_pct: float,
    short_delta: float,
    pnl_scaled: float,
    max_loss_total: float,
) -> Tuple[str, List[str]]:
    loss_total = max(0.0, -pnl_scaled)
    abs_short_delta = abs(short_delta)

    if (
        distance_pct < 0.005
        or abs_short_delta > 0.55
        or (max_loss_total > 0 and loss_total > 0.70 * max_loss_total)
    ):
        return "EXIT", [f"{spread_type}_EXTREME_RISK_EXIT"]

    if (
        distance_pct < 0.02
        or abs_short_delta > 0.40
        or (max_loss_total > 0 and loss_total > 0.40 * max_loss_total)
    ):
        return "ADJUST", [f"{spread_type}_MODERATE_RISK_ADJUST"]

    if distance_pct < 0.04 or abs_short_delta > 0.30:
        return "WATCH", [f"{spread_type}_EARLY_WARNING_WATCH"]

    return "HOLD", [f"{spread_type}_WITHIN_LIMITS"]


def print_portfolio_risk_table(
    kite: KiteConnect,
    user_id: str,
    symbols: List[str],
    risk_free_rate: float,
) -> None:
    positions = fetch_positions(kite)
    holdings = fetch_holdings_safe(kite)

    rows = []

    for symbol in symbols:
        try:
            credit_spreads = detect_credit_spreads(positions, symbol)
            strangles = detect_short_strangles(positions, symbol)
            covered_calls = detect_covered_calls(positions, symbol, holdings=holdings)
            covered_puts = detect_covered_puts(positions, symbol)
            naked_options = detect_naked_options(positions, symbol, holdings=holdings)

            # ------------------------------------------------------------
            # STRANGLES
            # ------------------------------------------------------------
            for strangle in strangles:
                snapshot, _ = build_snapshot(
                    kite=kite,
                    positions=positions,
                    user_id=user_id,
                    strangle=strangle,
                    baseline_iv=None,
                    risk_free_rate=risk_free_rate,
                )

                qty = strangle.quantity

                put_premium = float(strangle.short_put.average_price or 0.0)
                call_premium = float(strangle.short_call.average_price or 0.0)
                total_premium_unit = put_premium + call_premium

                max_profit_total = total_premium_unit * qty

                put_exp_at_spot = expiry_pnl_at_spot_for_short_option(
                    option_type="PE",
                    strike=strangle.short_put.strike,
                    spot=snapshot.spot,
                    premium=put_premium,
                    quantity=qty,
                )

                call_exp_at_spot = expiry_pnl_at_spot_for_short_option(
                    option_type="CE",
                    strike=strangle.short_call.strike,
                    spot=snapshot.spot,
                    premium=call_premium,
                    quantity=qty,
                )

                exp_at_spot = put_exp_at_spot + call_exp_at_spot
                captured_pct = (
                    snapshot.pnl_scaled / max_profit_total * 100.0
                    if max_profit_total
                    else 0.0
                )

                nearest_dist_pct = min(snapshot.dist_put_pct, snapshot.dist_call_pct) * 100.0

                rows.append({
                    "symbol": symbol,
                    "type": "STRANGLE",
                    "structure": f"{snapshot.short_put_strike}PE/{snapshot.short_call_strike}CE",
                    "spot": snapshot.spot,
                    "delta": snapshot.net_delta,
                    "dist": nearest_dist_pct,
                    "zone": risk_zone_from_distance(nearest_dist_pct),
                    "risk": snapshot.risk_score,
                    "pnl": snapshot.pnl_scaled,
                    "expiry_pnl": max_profit_total,
                    "expiry_pnl_at_spot": exp_at_spot,
                    "capture_pct": captured_pct,
                    "decision": snapshot.decision,
                    "reason": "|".join(snapshot.reason_codes),
                })

            # ------------------------------------------------------------
            # CREDIT SPREADS
            # ------------------------------------------------------------
            for spread in credit_spreads:
                short_leg = spread.short_leg
                long_leg = spread.long_leg
                qty = spread.quantity

                spot_instrument = spot_instrument_for(symbol)
                future_symbol = choose_nearest_future_symbol(positions, symbol, short_leg.expiry_code)
                future_instrument = f"NFO:{future_symbol}"
                short_instrument = f"NFO:{short_leg.tradingsymbol}"
                long_instrument = f"NFO:{long_leg.tradingsymbol}"

                ltp = get_ltp_map(
                    kite,
                    [spot_instrument, future_instrument, short_instrument, long_instrument],
                )

                spot = ltp[spot_instrument]
                future = ltp[future_instrument]
                short_ltp = ltp[short_instrument]
                long_ltp = ltp[long_instrument]

                t = year_fraction_to_market_close(short_leg.expiry_date)

                short_iv = implied_volatility(
                    short_ltp,
                    future,
                    short_leg.strike,
                    t,
                    risk_free_rate,
                    short_leg.option_type,
                )
                long_iv = implied_volatility(
                    long_ltp,
                    future,
                    long_leg.strike,
                    t,
                    risk_free_rate,
                    long_leg.option_type,
                )

                short_g = greeks_forward(
                    future,
                    short_leg.strike,
                    t,
                    risk_free_rate,
                    short_iv,
                    short_leg.option_type,
                )
                long_g = greeks_forward(
                    future,
                    long_leg.strike,
                    t,
                    risk_free_rate,
                    long_iv,
                    long_leg.option_type,
                )

                # Spread Greeks = short leg Greeks plus long hedge Greeks.
                net_delta = -short_g.delta + long_g.delta
                net_gamma = -short_g.gamma + long_g.gamma
                short_delta = -short_g.delta

                short_premium = float(short_leg.average_price or 0.0)
                long_premium = float(long_leg.average_price or 0.0)

                entry_credit_unit = short_premium - long_premium
                current_credit_unit = short_ltp - long_ltp
                pnl_unit = entry_credit_unit - current_credit_unit
                pnl_scaled = pnl_unit * qty

                width = abs(short_leg.strike - long_leg.strike)
                max_profit_total = entry_credit_unit * qty
                max_loss_total = max(width - entry_credit_unit, 0.0) * qty

                short_exp_at_spot = expiry_pnl_at_spot_for_short_option(
                    option_type=short_leg.option_type,
                    strike=short_leg.strike,
                    spot=spot,
                    premium=short_premium,
                    quantity=qty,
                )
                long_exp_at_spot = expiry_pnl_at_spot_for_long_option(
                    option_type=long_leg.option_type,
                    strike=long_leg.strike,
                    spot=spot,
                    premium=long_premium,
                    quantity=qty,
                )
                exp_at_spot = short_exp_at_spot + long_exp_at_spot

                if spread.spread_type == "BEAR_CALL":
                    distance_pct = (short_leg.strike - spot) / spot
                    structure = f"{short_leg.strike}CE/{long_leg.strike}CE"
                else:
                    distance_pct = (spot - short_leg.strike) / spot
                    structure = f"{short_leg.strike}PE/{long_leg.strike}PE"

                captured_pct = (
                    pnl_scaled / max_profit_total * 100.0
                    if max_profit_total
                    else 0.0
                )

                risk_score = abs(net_gamma) / max(abs(distance_pct), 0.0001)

                decision, reasons = evaluate_credit_spread_risk(
                    spread_type=spread.spread_type,
                    distance_pct=distance_pct,
                    short_delta=short_delta,
                    pnl_scaled=pnl_scaled,
                    max_loss_total=max_loss_total,
                )

                rows.append({
                    "symbol": symbol,
                    "type": spread.spread_type,
                    "structure": structure,
                    "spot": spot,
                    "delta": net_delta,
                    "dist": distance_pct * 100.0,
                    "zone": risk_zone_from_distance(distance_pct * 100.0),
                    "risk": risk_score,
                    "pnl": pnl_scaled,
                    "expiry_pnl": max_profit_total,
                    "expiry_pnl_at_spot": exp_at_spot,
                    "capture_pct": captured_pct,
                    "decision": decision,
                    "reason": "|".join(reasons),
                })

            # ------------------------------------------------------------
            # COVERED CALLS
            # ------------------------------------------------------------
            for cc in covered_calls:
                leg = cc.leg
                qty = cc.quantity

                spot_instrument = spot_instrument_for(symbol)
                future_symbol = choose_nearest_future_symbol(positions, symbol, leg.expiry_code)
                future_instrument = f"NFO:{future_symbol}"
                option_instrument = f"NFO:{leg.tradingsymbol}"

                ltp = get_ltp_map(kite, [spot_instrument, future_instrument, option_instrument])

                spot = ltp[spot_instrument]
                future = ltp[future_instrument]
                option_ltp = ltp[option_instrument]

                t = year_fraction_to_market_close(leg.expiry_date)

                iv = implied_volatility(
                    option_ltp,
                    future,
                    leg.strike,
                    t,
                    risk_free_rate,
                    leg.option_type,
                )

                greeks = greeks_forward(
                    future,
                    leg.strike,
                    t,
                    risk_free_rate,
                    iv,
                    leg.option_type,
                )

                call_delta = greeks.delta
                short_call_delta = -call_delta

                # Covered call = long underlying + short call.
                net_delta = 1.0 - call_delta

                premium = float(leg.average_price or 0.0)
                option_pnl_unit = premium - option_ltp

                cover_entry = None
                if cc.cover_type == "FUTURE":
                    for p in positions:
                        if p.get("tradingsymbol") == future_symbol and int(p.get("quantity") or 0) > 0:
                            cover_entry = float(p.get("average_price") or 0.0)
                            break
                    cover_price = future
                else:
                    for h in holdings:
                        if str(h.get("tradingsymbol", "")).upper() == symbol.upper():
                            cover_entry = float(h.get("average_price") or 0.0)
                            break
                    cover_price = spot

                if cover_entry is None:
                    underlying_pnl_unit = 0.0
                    underlying_expiry_pnl_unit = 0.0
                    max_profit_total = premium * qty
                else:
                    underlying_pnl_unit = cover_price - cover_entry
                    underlying_expiry_pnl_unit = spot - cover_entry

                    # Covered call max expiry profit when underlying closes at or above short call strike.
                    max_profit_total = ((leg.strike - cover_entry) + premium) * qty

                pnl_unit = underlying_pnl_unit + option_pnl_unit
                pnl_scaled = pnl_unit * qty

                option_exp_at_spot = expiry_pnl_at_spot_for_short_option(
                    option_type=leg.option_type,
                    strike=leg.strike,
                    spot=spot,
                    premium=premium,
                    quantity=qty,
                )
                underlying_exp_at_spot = underlying_expiry_pnl_unit * qty
                exp_at_spot = underlying_exp_at_spot + option_exp_at_spot

                distance_pct = (leg.strike - spot) / spot

                captured_pct = (
                    pnl_scaled / max_profit_total * 100.0
                    if max_profit_total
                    else 0.0
                )

                decision, reasons = evaluate_covered_call_risk(
                    distance_pct=distance_pct,
                    short_delta=short_call_delta,
                    pnl_unit=pnl_unit,
                    entry_premium_unit=premium,
                )

                rows.append({
                    "symbol": symbol,
                    "type": f"COV_CALL_{cc.cover_type}",
                    "structure": str(leg.strike),
                    "spot": spot,
                    "delta": net_delta,
                    "dist": distance_pct * 100.0,
                    "zone": risk_zone_from_distance(distance_pct * 100.0),
                    "risk": 0.0,
                    "pnl": pnl_scaled,
                    "expiry_pnl": max_profit_total,
                    "expiry_pnl_at_spot": exp_at_spot,
                    "capture_pct": captured_pct,
                    "decision": decision,
                    "reason": "|".join(reasons),
                })

            # ------------------------------------------------------------
            # COVERED PUTS
            # ------------------------------------------------------------
            for cp in covered_puts:
                leg = cp.leg
                qty = cp.quantity

                spot_instrument = spot_instrument_for(symbol)
                future_symbol = choose_nearest_future_symbol(positions, symbol, leg.expiry_code)
                future_instrument = f"NFO:{future_symbol}"
                option_instrument = f"NFO:{leg.tradingsymbol}"

                ltp = get_ltp_map(kite, [spot_instrument, future_instrument, option_instrument])

                spot = ltp[spot_instrument]
                future = ltp[future_instrument]
                option_ltp = ltp[option_instrument]

                t = year_fraction_to_market_close(leg.expiry_date)

                iv = implied_volatility(
                    option_ltp,
                    future,
                    leg.strike,
                    t,
                    risk_free_rate,
                    leg.option_type,
                )

                greeks = greeks_forward(
                    future,
                    leg.strike,
                    t,
                    risk_free_rate,
                    iv,
                    leg.option_type,
                )

                # Covered put = short future + short put.
                # Long PE delta is negative. Short PE delta is positive.
                short_put_delta = -greeks.delta
                net_delta = -1.0 + short_put_delta

                premium = float(leg.average_price or 0.0)
                option_pnl_unit = premium - option_ltp

                future_entry = None
                for p in positions:
                    if p.get("tradingsymbol") == future_symbol and int(p.get("quantity") or 0) < 0:
                        future_entry = float(p.get("average_price") or 0.0)
                        break

                if future_entry is None:
                    future_pnl_unit = 0.0
                    future_expiry_pnl_unit = 0.0
                    max_profit_total = premium * qty
                else:
                    # Short future PnL = entry - current.
                    future_pnl_unit = future_entry - future
                    future_expiry_pnl_unit = future_entry - spot

                    # Covered put max expiry profit when underlying closes at or below short put strike.
                    max_profit_total = ((future_entry - leg.strike) + premium) * qty

                pnl_unit = future_pnl_unit + option_pnl_unit
                pnl_scaled = pnl_unit * qty

                option_exp_at_spot = expiry_pnl_at_spot_for_short_option(
                    option_type=leg.option_type,
                    strike=leg.strike,
                    spot=spot,
                    premium=premium,
                    quantity=qty,
                )
                future_exp_at_spot = future_expiry_pnl_unit * qty
                exp_at_spot = future_exp_at_spot + option_exp_at_spot

                distance_pct = (spot - leg.strike) / spot

                captured_pct = (
                    pnl_scaled / max_profit_total * 100.0
                    if max_profit_total
                    else 0.0
                )

                decision, reasons = evaluate_covered_put_risk(
                    distance_pct=distance_pct,
                    net_delta=net_delta,
                    pnl_unit=pnl_unit,
                    entry_premium_unit=premium,
                )

                rows.append({
                    "symbol": symbol,
                    "type": f"COV_PUT_{cp.cover_type}",
                    "structure": str(leg.strike),
                    "spot": spot,
                    "delta": net_delta,
                    "dist": distance_pct * 100.0,
                    "zone": risk_zone_from_distance(distance_pct * 100.0),
                    "risk": 0.0,
                    "pnl": pnl_scaled,
                    "expiry_pnl": max_profit_total,
                    "expiry_pnl_at_spot": exp_at_spot,
                    "capture_pct": captured_pct,
                    "decision": decision,
                    "reason": "|".join(reasons),
                })

            # ------------------------------------------------------------
            # NAKED OPTIONS
            # ------------------------------------------------------------
            for naked in naked_options:
                leg = naked.leg
                qty = naked.quantity

                spot_instrument = spot_instrument_for(symbol)
                future_symbol = choose_nearest_future_symbol(positions, symbol, leg.expiry_code)
                future_instrument = f"NFO:{future_symbol}"
                option_instrument = f"NFO:{leg.tradingsymbol}"

                ltp = get_ltp_map(kite, [spot_instrument, future_instrument, option_instrument])

                spot = ltp[spot_instrument]
                future = ltp[future_instrument]
                option_ltp = ltp[option_instrument]

                t = year_fraction_to_market_close(leg.expiry_date)

                iv = implied_volatility(
                    option_ltp,
                    future,
                    leg.strike,
                    t,
                    risk_free_rate,
                    leg.option_type,
                )

                greeks = greeks_forward(
                    future,
                    leg.strike,
                    t,
                    risk_free_rate,
                    iv,
                    leg.option_type,
                )

                short_delta = -greeks.delta

                if leg.option_type == "CE":
                    distance_pct = (leg.strike - spot) / spot
                else:
                    distance_pct = (spot - leg.strike) / spot

                premium = float(leg.average_price or 0.0)
                pnl_unit = premium - option_ltp
                pnl_scaled = pnl_unit * qty

                max_profit_total = premium * qty
                exp_at_spot = expiry_pnl_at_spot_for_short_option(
                    option_type=leg.option_type,
                    strike=leg.strike,
                    spot=spot,
                    premium=premium,
                    quantity=qty,
                )

                captured_pct = (
                    pnl_scaled / max_profit_total * 100.0
                    if max_profit_total
                    else 0.0
                )

                decision, reasons = evaluate_naked_risk(
                    option_type=leg.option_type,
                    signed_short_delta=short_delta,
                    distance_pct=distance_pct,
                    iv_change_points=0.0,
                    pnl_unit=pnl_unit,
                    entry_premium_unit=premium,
                )

                rows.append({
                    "symbol": symbol,
                    "type": f"NAKED_{leg.option_type}",
                    "structure": str(leg.strike),
                    "spot": spot,
                    "delta": short_delta,
                    "dist": distance_pct * 100.0,
                    "zone": risk_zone_from_distance(distance_pct * 100.0),
                    "risk": 0.0,
                    "pnl": pnl_scaled,
                    "expiry_pnl": max_profit_total,
                    "expiry_pnl_at_spot": exp_at_spot,
                    "capture_pct": captured_pct,
                    "decision": decision,
                    "reason": "|".join(reasons),
                })

            if not credit_spreads and not strangles and not covered_calls and not covered_puts and not naked_options:
                rows.append({
                    "symbol": symbol,
                    "type": "-",
                    "structure": "-",
                    "spot": 0.0,
                    "delta": 0.0,
                    "dist": 0.0,
                    "zone": "-",
                    "risk": 0.0,
                    "pnl": 0.0,
                    "expiry_pnl": 0.0,
                    "expiry_pnl_at_spot": 0.0,
                    "capture_pct": 0.0,
                    "decision": "NONE",
                    "reason": "NO_SHORT_OPTION_POSITION",
                })

        except Exception as exc:
            rows.append({
                "symbol": symbol,
                "type": "ERROR",
                "structure": "-",
                "spot": 0.0,
                "delta": 0.0,
                "dist": 0.0,
                "zone": "ERROR",
                "risk": 0.0,
                "pnl": 0.0,
                "expiry_pnl": 0.0,
                "expiry_pnl_at_spot": 0.0,
                "capture_pct": 0.0,
                "decision": "ERROR",
                "reason": f"{type(exc).__name__}: {exc}",
            })

    priority = {"EXIT": 0, "ADJUST": 1, "WATCH": 2, "HOLD": 3, "NONE": 4, "ERROR": 5}
    zone_priority = {"ITM": 0, "DANGER": 1, "NEAR": 2, "WATCH": 3, "SAFE": 4, "-": 5, "ERROR": 6}

    rows.sort(
        key=lambda r: (
            priority.get(r["decision"], 9),
            zone_priority.get(r.get("zone", "-"), 9),
            -abs(r.get("delta", 0.0)),
            r.get("dist", 999.0),
            r["symbol"],
            r["type"],
        )
    )

    print("\nPORTFOLIO OPTION RISK SNAPSHOT")
    print("-" * 176)
    print(
        f"{'Symbol':12} | {'Type':16} | {'Structure':16} | "
        f"{'Spot':>9} | {'Delta':>8} | {'Dist%':>7} | "
        f"{'Zone':>7} | {'Risk':>6} | {'PnL':>11} | "
        f"{'ExpPnL':>9} | {'Exp@Spot':>10} | {'Cap%':>6} | "
        f"{'Decision':>8} | Reason"
    )
    print("-" * 176)

    for r in rows:
        print(
            f"{r['symbol']:12} | "
            f"{r['type']:16} | "
            f"{r['structure']:16} | "
            f"{r['spot']:9.2f} | "
            f"{r['delta']:+8.4f} | "
            f"{r['dist']:7.2f} | "
            f"{r.get('zone', '-'):7} | "
            f"{r['risk']:6.3f} | "
            f"{r['pnl']:11.2f} | "
            f"{r.get('expiry_pnl', 0.0):9.0f} | "
            f"{r.get('expiry_pnl_at_spot', 0.0):10.0f} | "
            f"{r.get('capture_pct', 0.0):6.1f}% | "
            f"{r['decision']:8} | "
            f"{r['reason']}"
        )

    print("-" * 176)



# ============================================================
# Runtime
# ============================================================

def is_market_open(open_hhmm: str, close_hhmm: str) -> bool:
    open_hh, open_mm = open_hhmm.split(":")
    close_hh, close_mm = close_hhmm.split(":")

    open_time = dtime(hour=int(open_hh), minute=int(open_mm))
    close_time = dtime(hour=int(close_hh), minute=int(close_mm))
    now_time = datetime.now().time()

    return open_time <= now_time <= close_time


def is_before_market_close(close_hhmm: str) -> bool:
    hh, mm = close_hhmm.split(":")
    close_time = dtime(hour=int(hh), minute=int(mm))
    return datetime.now().time() <= close_time


def run_once(
    kite: KiteConnect,
    user_id: str,
    symbol: str,
    baseline_iv: Optional[Dict[str, float]],
    risk_free_rate: float,
    log_path: Path,
) -> Dict[str, float]:
    positions = fetch_positions(kite)
    holdings = fetch_holdings_safe(kite)
    strangles = detect_short_strangles(positions, symbol)
    naked_options = detect_naked_options(positions, symbol, holdings=holdings)

    if naked_options:
        print(
            f"{datetime.now().isoformat(timespec='seconds')} | "
            f"{user_id} | {symbol} | NAKED_OPTIONS_FOUND={len(naked_options)}"
        )
        for naked in naked_options:
            monitor_naked_option_once(
                kite=kite,
                positions=positions,
                user_id=user_id,
                naked=naked,
                risk_free_rate=risk_free_rate,
            )

    if not strangles:
        if not naked_options:
            msg = f"{datetime.now().isoformat(timespec='seconds')} | {user_id} | {symbol} | NO_SHORT_STRANGLE_OR_NAKED_OPTION_FOUND"
            print(msg)
        return baseline_iv or {}

    if len(strangles) > 1:
        print(f"WARNING: Multiple strangles detected for {symbol}. Monitoring first detected strangle only.")

    strangle = strangles[0]

    snapshot, new_baseline_iv = build_snapshot(
        kite=kite,
        positions=positions,
        user_id=user_id,
        strangle=strangle,
        baseline_iv=baseline_iv,
        risk_free_rate=risk_free_rate,
    )

    if not is_data_sane(snapshot):
        print(f"{datetime.now().isoformat()} | DATA_REJECTED | Bad market data detected")
        return baseline_iv or {}

    previous_snapshot = read_previous_snapshot(log_path)
    apply_snapshot_changes(snapshot, previous_snapshot)

    print_snapshot(snapshot)
    append_snapshot(log_path, snapshot)
    print_history(log_path, last_n=5)

    return new_baseline_iv


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Short strangle risk monitor")
    parser.add_argument("--user-id", required=True, help="Zerodha user id, e.g. OMK569")
    parser.add_argument("--symbols", required=False, help="Comma-separated symbols, e.g. TRENT,RELIANCE,INFY")
    parser.add_argument("--symbol", required=False, help="Single underlying symbol, e.g. TRENT. Kept for backward compatibility.")
    parser.add_argument("--all", action="store_true", help="Monitor all symbols with short option positions for this user")
    parser.add_argument("--interval", type=int, default=DEFAULT_INTERVAL_SECONDS, help="Monitor interval in seconds")
    parser.add_argument("--once", action="store_true", help="Run once and exit")
    parser.add_argument("--risk-free-rate", type=float, default=DEFAULT_RISK_FREE_RATE)
    parser.add_argument("--market-open", default=DEFAULT_MARKET_OPEN, help="HH:MM, default 09:15")
    parser.add_argument("--market-close", default=DEFAULT_MARKET_CLOSE, help="HH:MM, default 15:30")
    parser.add_argument("--force", action="store_true", help="Allow run outside market hours for testing only")
    parser.add_argument("--table", action="store_true", help="Print compact portfolio snapshot table")
    parser.add_argument("--log-dir", default=DEFAULT_LOG_DIR)
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    user_id = args.user_id.upper().strip()

    kite = make_kite(user_id)

    if args.all:
        positions = fetch_positions(kite)
        symbols = detect_short_option_underlyings(positions)
        if not symbols:
            raise SystemExit(f"No short option positions found for user {user_id}")
    else:
        raw_symbols = args.symbols or args.symbol
        if not raw_symbols:
            raise SystemExit("Please provide --symbol TRENT, --symbols TRENT,RELIANCE, or --all")
        symbols = [s.strip().upper() for s in raw_symbols.split(",") if s.strip()]

    baseline_iv_map: Dict[str, Dict[str, float]] = {}

    if args.table:
        print_portfolio_risk_table(
            kite=kite,
            user_id=user_id,
            symbols=symbols,
            risk_free_rate=args.risk_free_rate,
        )
        return 0

    if args.once:
        if not args.force and not is_market_open(args.market_open, args.market_close):
            print(
                f"{datetime.now().isoformat(timespec='seconds')} | MARKET_CLOSED | Showing last snapshot"
            )
            for symbol in symbols:
                log_path = Path(args.log_dir) / f"strategy_risk_{user_id}_{symbol}.csv"
                print_last_snapshot_from_log(log_path)
            return 0

        for symbol in symbols:
            log_path = Path(args.log_dir) / f"strategy_risk_{user_id}_{symbol}.csv"
            baseline_iv_map[symbol] = run_once(
                kite=kite,
                user_id=user_id,
                symbol=symbol,
                baseline_iv=baseline_iv_map.get(symbol),
                risk_free_rate=args.risk_free_rate,
                log_path=log_path,
            )
        return 0

    print(
        f"Starting strategy risk monitor | user={user_id} | "
        f"symbols={','.join(symbols)} | interval={args.interval}s"
    )

    while is_before_market_close(args.market_close):
        if not args.force and not is_market_open(args.market_open, args.market_close):
            print(
                f"{datetime.now().isoformat(timespec='seconds')} | MARKET_CLOSED | "
                f"Waiting for market window {args.market_open}-{args.market_close}"
            )
            time.sleep(args.interval)
            continue

        for symbol in symbols:
            try:
                log_path = Path(args.log_dir) / f"strategy_risk_{user_id}_{symbol}.csv"
                baseline_iv_map[symbol] = run_once(
                    kite=kite,
                    user_id=user_id,
                    symbol=symbol,
                    baseline_iv=baseline_iv_map.get(symbol),
                    risk_free_rate=args.risk_free_rate,
                    log_path=log_path,
                )
            except Exception as exc:
                print(
                    f"{datetime.now().isoformat(timespec='seconds')} | "
                    f"{user_id} | {symbol} | ERROR | {type(exc).__name__}: {exc}"
                )

        time.sleep(args.interval)

    print(f"Market close reached: {args.market_close}. Monitor stopped.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
