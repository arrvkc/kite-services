from __future__ import annotations

import json
from pathlib import Path
from typing import List, Optional, Tuple

from kiteconnect import KiteConnect

from services.kite_credentials_service import get_kite_credentials
from services.kite_market_data_service import get_all_futures_positions
from engines.exit_positions.models import Config, GttRecord, PositionRecord


class BulkExitKiteAdapter:
    def __init__(self, audit_root: str = 'services/audit_logs/bulk_exit') -> None:
        self.audit_root = Path(audit_root)

    def _get_kite(self, user_id: str) -> KiteConnect:
        api_key, access_token = get_kite_credentials(user_id)
        kite = KiteConnect(api_key=api_key)
        kite.set_access_token(access_token)
        return kite

    def fetch_positions_for_clients(self, client_ids: List[str]) -> List[PositionRecord]:
        rows: List[PositionRecord] = []
        for client_id in client_ids:
            for item in get_all_futures_positions(client_id, exclude_zero_qty=True):
                rows.append(
                    PositionRecord(
                        client_id=client_id,
                        exchange='NFO',
                        underlying_symbol=str(item.get('underlying', '')),
                        tradingsymbol=str(item['tradingsymbol']),
                        product='NRML',
                        net_quantity=int(item.get('quantity', 0)),
                        average_price=float(item['avg_price']) if item.get('avg_price') is not None else None,
                        pnl=float(item['pnl']) if item.get('pnl') is not None else None,
                        expiry=str(item.get('expiry')) if item.get('expiry') is not None else None,
                        tick_size=float(item['tick_size']) if item.get('tick_size') is not None else None,
                        instrument_token=int(item['instrument_token']) if item.get('instrument_token') is not None else None,
                    )
                )
        return rows

    def refetch_position(self, client_id: str, exchange: str, tradingsymbol: str) -> Optional[PositionRecord]:
        rows = self.fetch_positions_for_clients([client_id])
        for row in rows:
            if row.exchange.upper() == exchange.upper() and row.tradingsymbol.upper() == tradingsymbol.upper():
                return row
        return None

    def fetch_linked_gtts(self, config: Config, matched_positions: List[PositionRecord]) -> List[GttRecord]:
        # Safety choice: only single-leg active stop GTTs are considered cancellable.
        out: List[GttRecord] = []
        client_ids = sorted({p.client_id for p in matched_positions})
        for client_id in client_ids:
            kite = self._get_kite(client_id)
            gtts = kite.get_gtts()
            for g in gtts:
                status = g.get('status')
                if str(status).lower() != 'active':
                    continue
                orders = g.get('orders') or []
                condition = g.get('condition') or {}
                if len(orders) != 1:
                    continue
                order = orders[0]
                out.append(
                    GttRecord(
                        client_id=client_id,
                        trigger_id=str(g.get('id')),
                        exchange=str(condition.get('exchange') or order.get('exchange') or ''),
                        tradingsymbol=str(condition.get('tradingsymbol') or order.get('tradingsymbol') or ''),
                        transaction_type=str(order.get('transaction_type') or ''),
                        product=order.get('product'),
                        trigger_values=[float(x) for x in (condition.get('trigger_values') or [])],
                        last_price=float(condition['last_price']) if condition.get('last_price') is not None else None,
                        status=str(status),
                        raw=g,
                    )
                )
        from engines.exit_positions.planner import match_single_leg_stop_gtts
        return match_single_leg_stop_gtts(config, matched_positions, out)

    def cancel_gtt(self, client_id: str, trigger_id: str) -> str:
        kite = self._get_kite(client_id)
        result = kite.delete_gtt(int(trigger_id))
        if isinstance(result, dict) and result.get('trigger_id') is not None:
            return f'Cancelled trigger_id={result.get("trigger_id")}'
        return 'Cancelled'

    def place_market_exit_order(
        self,
        client_id: str,
        exchange: str,
        tradingsymbol: str,
        transaction_type: str,
        quantity: int,
        product: Optional[str],
    ) -> Tuple[Optional[str], str]:
        kite = self._get_kite(client_id)
        order_id = kite.place_order(
            variety=kite.VARIETY_REGULAR,
            exchange=exchange,
            tradingsymbol=tradingsymbol,
            transaction_type=transaction_type,
            quantity=quantity,
            product=product or kite.PRODUCT_NRML,
            order_type=kite.ORDER_TYPE_MARKET,
            validity=kite.VALIDITY_DAY,
        )
        return str(order_id), 'Order accepted'

    def write_audit_record(self, record: dict) -> None:
        self.audit_root.mkdir(parents=True, exist_ok=True)
        path = self.audit_root / f"{record['operation_id']}.json"
        path.write_text(json.dumps(record, indent=2), encoding='utf-8')
