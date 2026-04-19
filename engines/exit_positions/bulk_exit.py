from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict
from pathlib import Path
from typing import List

from .executor import run_exit_orders, run_gtt_cancellations
from .models import Config, OperationRecord, Summary
from .planner import (
    NoMatchingPositionsError,
    SafetyThresholdError,
    build_orders,
    enforce_thresholds,
    filter_positions,
    match_single_leg_stop_gtts,
)
from services.bulk_exit_kite_adapter import BulkExitKiteAdapter


EXIT_CODES = {
    'SUCCESS': 0,
    'GENERAL_RUNTIME_FAILURE': 1,
    'ARGUMENT_VALIDATION_ERROR': 2,
    'NO_MATCHING_POSITIONS_FOUND': 3,
    'CONFIRMATION_FAILURE': 4,
    'SAFETY_THRESHOLD_EXCEEDED': 5,
    'CLIENT_RESOLUTION_FAILURE': 6,
    'POSITION_FETCH_FAILURE': 7,
    'PARTIAL_EXECUTION_FAILURE': 8,
    'AUDIT_WRITE_FAILURE': 9,
    'GTT_FETCH_FAILURE': 10,
    'GTT_CANCEL_PARTIAL_FAILURE': 11,
}


class CliError(Exception):
    def __init__(self, code_key: str, message: str):
        super().__init__(message)
        self.code_key = code_key
        self.message = message



def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description='Bulk Futures Position Liquidation and Linked GTT Cancellation CLI')
    p.add_argument('--scope', required=True, choices=['all-clients', 'client-list'])
    p.add_argument('--clients', required=True, help='Comma-separated Zerodha user IDs. Required for now for both scopes.')
    p.add_argument('--segment', required=True, choices=['futures'])
    p.add_argument('--side', required=True, choices=['all', 'long', 'short'])
    p.add_argument('--symbol')
    p.add_argument('--exchange', choices=['NFO', 'BFO'])
    p.add_argument('--product', choices=['NRML', 'MIS'])
    p.add_argument('--dry-run', action='store_true')
    p.add_argument('--execute', action='store_true')
    p.add_argument('--confirm')
    p.add_argument('--cancel-gtt', action='store_true')
    p.add_argument('--parallel-workers', type=int, default=8)
    p.add_argument('--pause-ms', type=int, default=0)
    p.add_argument('--max-orders', type=int)
    p.add_argument('--max-clients', type=int)
    p.add_argument('--max-gtt-cancel', type=int)
    p.add_argument('--retry-attempts', type=int, default=1)
    p.add_argument('--reason')
    p.add_argument('--operation-tag')
    p.add_argument('--format', dest='output_format', choices=['table', 'json'], default='table')
    p.add_argument('--output')
    p.add_argument('--verbose', action='store_true')
    p.add_argument('--quiet', action='store_true')
    return p



def parse_config(argv: List[str]) -> Config:
    args = build_parser().parse_args(argv)
    if args.dry_run and args.execute:
        raise CliError('ARGUMENT_VALIDATION_ERROR', '--dry-run and --execute are mutually exclusive')
    if args.verbose and args.quiet:
        raise CliError('ARGUMENT_VALIDATION_ERROR', '--verbose and --quiet are mutually exclusive')
    if args.parallel_workers < 1:
        raise CliError('ARGUMENT_VALIDATION_ERROR', '--parallel-workers must be >= 1')
    if not (0 <= args.retry_attempts <= 3):
        raise CliError('ARGUMENT_VALIDATION_ERROR', '--retry-attempts must be in range 0..3')

    clients = []
    seen = set()
    for raw in args.clients.split(','):
        client_id = raw.strip()
        if client_id and client_id not in seen:
            seen.add(client_id)
            clients.append(client_id)
    if not clients:
        raise CliError('CLIENT_RESOLUTION_FAILURE', 'At least one client id must be supplied in --clients')

    mode = 'execute' if args.execute else 'dry_run'
    if mode == 'execute':
        if not args.reason:
            raise CliError('ARGUMENT_VALIDATION_ERROR', '--reason is required in live mode')
        expected = 'EXIT_FUTURES_AND_GTT' if args.cancel_gtt else 'EXIT_FUTURES'
        if args.confirm != expected:
            raise CliError('CONFIRMATION_FAILURE', f'Incorrect confirmation token. Expected {expected}')

    return Config(
        scope=args.scope,
        clients=clients,
        segment=args.segment,
        side=args.side,
        symbol=args.symbol,
        exchange=args.exchange,
        product=args.product,
        mode=mode,
        confirm=args.confirm,
        cancel_gtt=args.cancel_gtt,
        parallel_workers=args.parallel_workers,
        pause_ms=args.pause_ms,
        max_orders=args.max_orders,
        max_clients=args.max_clients,
        max_gtt_cancel=args.max_gtt_cancel,
        retry_attempts=args.retry_attempts,
        reason=args.reason,
        operation_tag=args.operation_tag,
        output_format=args.output_format,
        output_path=args.output,
        verbose=args.verbose,
        quiet=args.quiet,
    )



def build_operation_id() -> str:
    from datetime import datetime, timezone
    return 'bulk_exit_' + datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')



def _write_output(config: Config, payload: str) -> None:
    print(payload)
    if config.output_path:
        Path(config.output_path).write_text(payload, encoding='utf-8')



def _table(headers, rows):
    widths = [len(h) for h in headers]
    for row in rows:
        for i, v in enumerate(row):
            widths[i] = max(widths[i], len(str(v)))
    def fmt(row):
        return ' | '.join(str(v).ljust(widths[i]) for i, v in enumerate(row))
    out = [fmt(headers), '-+-'.join('-' * w for w in widths)]
    out.extend(fmt(r) for r in rows)
    return '\n'.join(out)



def _render_table(operation_id, config, positions, gtts, orders, gtt_results, order_results):
    lines = []
    lines.append('BULK FUTURES EXIT - ' + ('LIVE EXECUTION' if config.is_live else 'DRY RUN'))
    lines.append('')
    lines.append(f'Operation ID : {operation_id}')
    lines.append(f'Scope        : {config.scope}')
    lines.append(f'Clients      : {",".join(config.clients)}')
    lines.append(f'Segment      : {config.segment}')
    lines.append(f'Side Filter  : {config.side}')
    lines.append(f'Symbol       : {config.symbol or "-"}')
    lines.append(f'Exchange     : {config.exchange or "-"}')
    lines.append('')
    pos_rows = [[p.client_id, p.exchange, p.tradingsymbol, p.net_quantity, p.position_side, p.product or '-'] for p in positions]
    lines.append('MATCHED POSITIONS')
    lines.append(_table(['Client','Exchange','Tradingsymbol','Net Qty','Position Side','Product'], pos_rows or [['-','-','-','-','-','-']]))
    lines.append('')
    if gtts:
        gtt_rows = [[g.client_id, g.tradingsymbol, g.trigger_id, g.transaction_type, g.last_price, g.trigger_values[0] if g.trigger_values else '-'] for g in gtts]
        lines.append('MATCHED GTTs FOR CANCELLATION')
        lines.append(_table(['Client','Tradingsymbol','Trigger ID','Side','Last Price','Trigger'], gtt_rows))
        lines.append('')
    order_rows = [[o.client_id, o.tradingsymbol, o.exit_side, o.quantity, o.order_type] for o in orders]
    lines.append('PLANNED EXIT ORDERS')
    lines.append(_table(['Client','Tradingsymbol','Exit Side','Qty','Order Type'], order_rows or [['-','-','-','-','-']]))
    lines.append('')
    if config.is_live and gtt_results:
        rows = [[r.client_id, r.tradingsymbol, r.trigger_id or '-', r.status, r.message] for r in gtt_results]
        lines.append('GTT CANCEL RESULTS')
        lines.append(_table(['Client','Tradingsymbol','Trigger ID','Status','Message'], rows))
        lines.append('')
    if config.is_live and order_results:
        rows = [[r.client_id, r.tradingsymbol, r.exit_side or '-', r.quantity or '-', r.status, r.broker_order_id or '-', r.message] for r in order_results]
        lines.append('ORDER RESULTS')
        lines.append(_table(['Client','Tradingsymbol','Exit Side','Qty','Status','Broker Order ID','Message'], rows))
        lines.append('')
    if not config.is_live:
        lines.append('NO LIVE ORDERS WERE PLACED.')
    return '\n'.join(str(x) for x in lines)



def main(argv: List[str] | None = None) -> int:
    argv = argv or sys.argv[1:]
    operation_id = build_operation_id()
    adapter = BulkExitKiteAdapter()
    try:
        config = parse_config(argv)
        positions_all = adapter.fetch_positions_for_clients(config.clients)
        matched_positions = filter_positions(config, positions_all)
        planned_orders = build_orders(matched_positions)
        matched_gtts = adapter.fetch_linked_gtts(config, matched_positions) if config.cancel_gtt else []
        enforce_thresholds(config, matched_positions, planned_orders, matched_gtts)

        summary = Summary(
            matched_clients_count=len({p.client_id for p in matched_positions}),
            matched_positions_count=len(matched_positions),
            planned_orders_count=len(planned_orders),
            matched_gtts_count=len(matched_gtts),
        )

        gtt_results = []
        order_results = []
        exit_code_key = 'SUCCESS'

        if config.is_live:
            if config.cancel_gtt:
                gtt_results = run_gtt_cancellations(config, matched_gtts, adapter.cancel_gtt)
                summary.gtt_cancel_success_count = sum(1 for r in gtt_results if r.status == 'SUCCESS')
                summary.gtt_cancel_failure_count = sum(1 for r in gtt_results if r.status == 'FAILED')
            order_results = run_exit_orders(config, planned_orders, adapter.refetch_position, adapter.place_market_exit_order)
            summary.order_success_count = sum(1 for r in order_results if r.status == 'SUCCESS')
            summary.order_failure_count = sum(1 for r in order_results if r.status == 'FAILED')
            summary.skipped_count = sum(1 for r in order_results if r.status == 'SKIPPED')
            if summary.order_failure_count > 0:
                exit_code_key = 'PARTIAL_EXECUTION_FAILURE'
            elif summary.gtt_cancel_failure_count > 0:
                exit_code_key = 'GTT_CANCEL_PARTIAL_FAILURE'

        record = OperationRecord(
            operation_id=operation_id,
            timestamp_utc=OperationRecord.now_utc(),
            mode=config.mode,
            raw_args=argv,
            filters={
                'scope': config.scope,
                'clients': config.clients,
                'segment': config.segment,
                'side': config.side,
                'symbol': config.symbol,
                'exchange': config.exchange,
                'product': config.product,
                'cancel_gtt': config.cancel_gtt,
                'parallel_workers': config.parallel_workers,
            },
            selected_clients=config.clients,
            matched_positions=[asdict(x) for x in matched_positions],
            matched_gtts=[asdict(x) for x in matched_gtts],
            planned_exit_orders=[asdict(x) for x in planned_orders],
            gtt_cancel_results=[asdict(x) for x in gtt_results],
            order_results=[asdict(x) for x in order_results],
            summary=asdict(summary),
            reason=config.reason,
            operation_tag=config.operation_tag,
            exit_code=EXIT_CODES[exit_code_key],
        )
        adapter.write_audit_record(record.to_dict())

        if config.output_format == 'json':
            payload = json.dumps(record.to_dict(), indent=2)
        else:
            payload = _render_table(operation_id, config, matched_positions, matched_gtts, planned_orders, gtt_results, order_results)
        _write_output(config, payload)
        return EXIT_CODES[exit_code_key]

    except NoMatchingPositionsError as exc:
        return _handle_error(operation_id, argv, 'NO_MATCHING_POSITIONS_FOUND', str(exc), output_path=None)
    except SafetyThresholdError as exc:
        return _handle_error(operation_id, argv, 'SAFETY_THRESHOLD_EXCEEDED', str(exc), output_path=None)
    except CliError as exc:
        return _handle_error(operation_id, argv, exc.code_key, exc.message, output_path=None)
    except Exception as exc:
        return _handle_error(operation_id, argv, 'GENERAL_RUNTIME_FAILURE', str(exc), output_path=None)



def _handle_error(operation_id: str, argv: List[str], code_key: str, message: str, output_path: str | None) -> int:
    payload = json.dumps({
        'operation_id': operation_id,
        'mode': 'error',
        'raw_args': argv,
        'error': message,
        'exit_code': EXIT_CODES[code_key],
    }, indent=2)
    print(payload)
    if output_path:
        Path(output_path).write_text(payload, encoding='utf-8')
    return EXIT_CODES[code_key]


if __name__ == '__main__':
    raise SystemExit(main())
