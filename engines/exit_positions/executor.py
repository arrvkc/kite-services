from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, Dict, Iterable, List, Tuple

from .models import Config, PlannedExitOrder, PositionRecord, TaskResult, GttRecord


def _pause(config: Config) -> None:
    if config.pause_ms > 0:
        time.sleep(config.pause_ms / 1000.0)


def run_gtt_cancellations(
    config: Config,
    gtts: Iterable[GttRecord],
    cancel_fn: Callable[[str, str], str],
) -> List[TaskResult]:
    gtts = list(gtts)
    if not gtts:
        return []

    def worker(g: GttRecord) -> TaskResult:
        _pause(config)
        try:
            message = cancel_fn(g.client_id, g.trigger_id)
            return TaskResult(
                client_id=g.client_id,
                tradingsymbol=g.tradingsymbol,
                trigger_id=g.trigger_id,
                status='SUCCESS',
                message=message or 'Cancelled',
            )
        except Exception as exc:
            return TaskResult(
                client_id=g.client_id,
                tradingsymbol=g.tradingsymbol,
                trigger_id=g.trigger_id,
                status='FAILED',
                message=str(exc),
            )

    results: List[TaskResult] = []
    with ThreadPoolExecutor(max_workers=config.parallel_workers) as pool:
        futures = [pool.submit(worker, g) for g in gtts]
        for fut in as_completed(futures):
            results.append(fut.result())
    return results


def run_exit_orders(
    config: Config,
    orders: Iterable[PlannedExitOrder],
    refetch_position_fn: Callable[[str, str, str], PositionRecord | None],
    place_order_fn: Callable[[str, str, str, str, int, str | None], Tuple[str | None, str]],
) -> List[TaskResult]:
    orders = list(orders)
    if not orders:
        return []

    def worker(order: PlannedExitOrder) -> TaskResult:
        _pause(config)
        try:
            refreshed = refetch_position_fn(order.client_id, order.exchange, order.tradingsymbol)
            if refreshed is None or refreshed.net_quantity == 0:
                return TaskResult(
                    client_id=order.client_id,
                    tradingsymbol=order.tradingsymbol,
                    exit_side=order.exit_side,
                    quantity=order.quantity,
                    status='SKIPPED',
                    message='Position already flat on refetch',
                )
            broker_order_id, message = place_order_fn(
                order.client_id,
                order.exchange,
                order.tradingsymbol,
                order.exit_side,
                order.quantity,
                order.product,
            )
            return TaskResult(
                client_id=order.client_id,
                tradingsymbol=order.tradingsymbol,
                exit_side=order.exit_side,
                quantity=order.quantity,
                broker_order_id=broker_order_id,
                status='SUCCESS',
                message=message,
            )
        except Exception as exc:
            return TaskResult(
                client_id=order.client_id,
                tradingsymbol=order.tradingsymbol,
                exit_side=order.exit_side,
                quantity=order.quantity,
                status='FAILED',
                message=str(exc),
            )

    results: List[TaskResult] = []
    with ThreadPoolExecutor(max_workers=config.parallel_workers) as pool:
        futures = [pool.submit(worker, order) for order in orders]
        for fut in as_completed(futures):
            results.append(fut.result())
    return results
