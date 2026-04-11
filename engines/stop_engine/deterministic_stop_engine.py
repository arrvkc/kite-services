#!/usr/bin/env python3
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional


@dataclass(frozen=True)
class StopComputationConfig:
    atr_period: int = 14
    atr_average_window: int = 20
    base_multiplier: float = 2.5
    volatility_spike_ratio: float = 2.0
    volatility_spike_multiplier_value: float = 3.25
    swing_lookback: int = 5
    min_distance_pct: float = 0.005
    min_distance_atr_multiple: float = 0.5
    material_change_pct: float = 0.0005
    material_change_tick_multiple: float = 2.0


def floor_to_tick(price: float, tick_size: float) -> float:
    if tick_size <= 0:
        raise ValueError("tick_size must be positive")
    units = int(price / tick_size)
    return round(units * tick_size, 8)


def ceil_to_tick(price: float, tick_size: float) -> float:
    if tick_size <= 0:
        raise ValueError("tick_size must be positive")
    units = int(price / tick_size)
    exact = units * tick_size
    if abs(exact - price) < 1e-12:
        return round(exact, 8)
    return round((units + 1) * tick_size, 8)


def _validate_candle(candle: Dict) -> None:
    required = {"date", "open", "high", "low", "close"}
    missing = required - set(candle.keys())
    if missing:
        raise ValueError(f"Candle missing fields: {sorted(missing)}")
    high = float(candle["high"])
    low = float(candle["low"])
    if high < low:
        raise ValueError(f"Invalid candle high/low: high={high}, low={low}")


def validate_candles(candles: List[Dict], config: StopComputationConfig) -> None:
    if not candles:
        raise ValueError("No candles provided")
    for candle in candles:
        _validate_candle(candle)
    min_needed = max(config.atr_period + config.atr_average_window, config.swing_lookback + 1)
    if len(candles) < min_needed:
        raise ValueError(f"Need at least {min_needed} completed candles, got {len(candles)}")


def true_range(high: float, low: float, prev_close: float) -> float:
    return max(high - low, abs(high - prev_close), abs(low - prev_close))


def compute_true_ranges(candles: List[Dict]) -> List[float]:
    if len(candles) < 2:
        return []
    trs: List[float] = []
    for i in range(1, len(candles)):
        trs.append(
            true_range(
                float(candles[i]["high"]),
                float(candles[i]["low"]),
                float(candles[i - 1]["close"]),
            )
        )
    return trs


def compute_atr(candles: List[Dict], period: int) -> float:
    trs = compute_true_ranges(candles)
    if len(trs) < period:
        raise ValueError(f"Need at least {period + 1} candles to compute ATR({period})")
    recent = trs[-period:]
    return sum(recent) / len(recent)


def compute_atr_average(candles: List[Dict], atr_period: int, avg_window: int) -> float:
    min_candles = atr_period + avg_window
    if len(candles) < min_candles:
        raise ValueError(f"Need at least {min_candles} candles to compute ATR average")
    atr_values: List[float] = []
    for end_idx in range(atr_period + 1, len(candles) + 1):
        atr_values.append(compute_atr(candles[:end_idx], atr_period))
    recent = atr_values[-avg_window:]
    return sum(recent) / len(recent)


def determine_effective_multiplier(atr: float, atr_average: float, config: StopComputationConfig) -> float:
    if atr_average > 0 and atr >= config.volatility_spike_ratio * atr_average:
        return config.volatility_spike_multiplier_value
    return config.base_multiplier


def compute_min_distance(current_price_reference: float, atr: float, config: StopComputationConfig) -> float:
    return max(current_price_reference * config.min_distance_pct, atr * config.min_distance_atr_multiple)


def material_change_required(
    old_trigger: Optional[float],
    new_trigger: float,
    current_price_reference: float,
    tick_size: float,
    config: StopComputationConfig,
) -> bool:
    if old_trigger is None:
        return True
    threshold = max(
        tick_size * config.material_change_tick_multiple,
        current_price_reference * config.material_change_pct,
    )
    return abs(new_trigger - old_trigger) >= threshold


def compute_deterministic_stop_eod(
    *,
    candles: List[Dict],
    side: str,
    tick_size: float,
    entry_price: float,
    previous_trigger_price: Optional[float] = None,
    config: StopComputationConfig = StopComputationConfig(),
) -> Dict:
    side = side.upper().strip()
    if side not in {"LONG", "SHORT"}:
        raise ValueError("side must be LONG or SHORT")
    if tick_size <= 0:
        raise ValueError("tick_size must be positive")
    if entry_price <= 0:
        raise ValueError("entry_price must be positive")

    validate_candles(candles, config)

    current_price_reference = float(candles[-1]["close"])
    atr = compute_atr(candles, config.atr_period)
    atr_average = compute_atr_average(candles, config.atr_period, config.atr_average_window)
    multiplier = determine_effective_multiplier(atr, atr_average, config)

    recent = candles[-config.swing_lookback:]
    swing_low = min(float(c["low"]) for c in recent)
    swing_high = max(float(c["high"]) for c in recent)
    min_distance = compute_min_distance(current_price_reference, atr, config)

    if side == "LONG":
        initial_stop = entry_price - (atr * multiplier)
        trailing_candidate = max(swing_low, current_price_reference - (atr * multiplier))
        raw_stop = max(initial_stop, trailing_candidate) if previous_trigger_price is None else max(previous_trigger_price, trailing_candidate)
        validated_stop = min(raw_stop, current_price_reference - min_distance)
        validated_stop = floor_to_tick(validated_stop, tick_size)
        if previous_trigger_price is not None:
            validated_stop = max(validated_stop, floor_to_tick(previous_trigger_price, tick_size))
        if validated_stop >= current_price_reference:
            validated_stop = floor_to_tick(current_price_reference - min_distance, tick_size)
        trigger_price = validated_stop
    else:
        initial_stop = entry_price + (atr * multiplier)
        trailing_candidate = min(swing_high, current_price_reference + (atr * multiplier))
        raw_stop = min(initial_stop, trailing_candidate) if previous_trigger_price is None else min(previous_trigger_price, trailing_candidate)
        validated_stop = max(raw_stop, current_price_reference + min_distance)
        validated_stop = ceil_to_tick(validated_stop, tick_size)
        if previous_trigger_price is not None:
            validated_stop = min(validated_stop, ceil_to_tick(previous_trigger_price, tick_size))
        if validated_stop <= current_price_reference:
            validated_stop = ceil_to_tick(current_price_reference + min_distance, tick_size)
        trigger_price = validated_stop

    update_required = material_change_required(
        old_trigger=previous_trigger_price,
        new_trigger=trigger_price,
        current_price_reference=current_price_reference,
        tick_size=tick_size,
        config=config,
    )

    return {
        "side": side,
        "entry_price": round(entry_price, 4),
        "current_price_reference": round(current_price_reference, 4),
        "atr": round(atr, 4),
        "atr_average": round(atr_average, 4),
        "multiplier": round(multiplier, 4),
        "swing_low": round(swing_low, 4),
        "swing_high": round(swing_high, 4),
        "initial_stop": round(initial_stop, 4),
        "trailing_candidate": round(trailing_candidate, 4),
        "previous_trigger_price": round(previous_trigger_price, 4) if previous_trigger_price is not None else None,
        "raw_stop": round(raw_stop, 4),
        "min_distance": round(min_distance, 4),
        "validated_stop": round(validated_stop, 8),
        "trigger_price": round(trigger_price, 8),
        "update_required": update_required,
    }


def prepare_limit_order_from_trigger(*, side: str, trigger_price: float, tick_size: float, current_price_reference: float, buffer_pct: float = 0.001) -> Dict:
    side = side.upper().strip()
    if side not in {"LONG", "SHORT"}:
        raise ValueError("side must be LONG or SHORT")
    if tick_size <= 0:
        raise ValueError("tick_size must be positive")
    if current_price_reference <= 0:
        raise ValueError("current_price_reference must be positive")
    buffer_value = max(current_price_reference * buffer_pct, tick_size)
    if side == "LONG":
        limit_price = floor_to_tick(trigger_price - buffer_value, tick_size)
        if limit_price > trigger_price:
            limit_price = trigger_price
    else:
        limit_price = ceil_to_tick(trigger_price + buffer_value, tick_size)
        if limit_price < trigger_price:
            limit_price = trigger_price
    return {
        "side": side,
        "trigger_price": round(trigger_price, 8),
        "buffer_value": round(buffer_value, 8),
        "limit_price": round(limit_price, 8),
    }
