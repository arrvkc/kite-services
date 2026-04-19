from __future__ import annotations

from typing import Iterable, List

from .models import Config, GttRecord, PlannedExitOrder, PositionRecord


class BulkExitPlanningError(Exception):
    pass


class NoMatchingPositionsError(BulkExitPlanningError):
    pass


class SafetyThresholdError(BulkExitPlanningError):
    pass


def _norm_symbol(value: str | None) -> str | None:
    if value is None:
        return None
    value = value.strip().upper()
    return value or None


def filter_positions(config: Config, positions: Iterable[PositionRecord]) -> List[PositionRecord]:
    symbol = _norm_symbol(config.symbol)
    out: List[PositionRecord] = []
    for p in positions:
        if p.net_quantity == 0:
            continue
        if config.side == 'long' and p.net_quantity <= 0:
            continue
        if config.side == 'short' and p.net_quantity >= 0:
            continue
        if config.exchange and p.exchange.upper() != config.exchange.upper():
            continue
        if config.product and (p.product or '').upper() != config.product.upper():
            continue
        if symbol and _norm_symbol(p.underlying_symbol) != symbol:
            continue
        out.append(p)
    return out


def build_orders(positions: Iterable[PositionRecord]) -> List[PlannedExitOrder]:
    orders: List[PlannedExitOrder] = []
    for p in positions:
        orders.append(
            PlannedExitOrder(
                client_id=p.client_id,
                exchange=p.exchange,
                tradingsymbol=p.tradingsymbol,
                exit_side='SELL' if p.net_quantity > 0 else 'BUY',
                quantity=abs(int(p.net_quantity)),
                product=p.product,
            )
        )
    return orders


def match_single_leg_stop_gtts(config: Config, positions: Iterable[PositionRecord], gtts: Iterable[GttRecord]) -> List[GttRecord]:
    matched: List[GttRecord] = []
    positions = list(positions)
    for g in gtts:
        if (g.status or '').lower() != 'active':
            continue
        for p in positions:
            if g.client_id != p.client_id:
                continue
            if g.exchange.upper() != p.exchange.upper():
                continue
            if g.tradingsymbol.upper() != p.tradingsymbol.upper():
                continue
            if config.product and (g.product or '').upper() != config.product.upper():
                continue
            expected_side = 'SELL' if p.net_quantity > 0 else 'BUY'
            if g.transaction_type.upper() != expected_side:
                continue
            # Safety rule: only single-leg stop-like triggers.
            # Long stop SELL should have trigger below last_price.
            # Short stop BUY should have trigger above last_price.
            if g.last_price is None or len(g.trigger_values) != 1:
                continue
            trig = float(g.trigger_values[0])
            if p.net_quantity > 0 and trig >= float(g.last_price):
                continue
            if p.net_quantity < 0 and trig <= float(g.last_price):
                continue
            matched.append(g)
    # Deduplicate by client + trigger_id
    dedup = {}
    for g in matched:
        dedup[(g.client_id, g.trigger_id)] = g
    return list(dedup.values())


def enforce_thresholds(config: Config, positions: List[PositionRecord], orders: List[PlannedExitOrder], gtts: List[GttRecord]) -> None:
    if not positions:
        raise NoMatchingPositionsError('No matching open futures positions found.')
    client_count = len({p.client_id for p in positions})
    if config.max_clients is not None and client_count > config.max_clients:
        raise SafetyThresholdError(f'Matched clients {client_count} exceeded --max-clients {config.max_clients}')
    if config.max_orders is not None and len(orders) > config.max_orders:
        raise SafetyThresholdError(f'Planned orders {len(orders)} exceeded --max-orders {config.max_orders}')
    if config.cancel_gtt and config.max_gtt_cancel is not None and len(gtts) > config.max_gtt_cancel:
        raise SafetyThresholdError(f'Planned GTT cancellations {len(gtts)} exceeded --max-gtt-cancel {config.max_gtt_cancel}')
