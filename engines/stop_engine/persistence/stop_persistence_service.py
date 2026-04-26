from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json

from engines.stop_engine.persistence.models import (
    StopEvent,
    StopState,
    StopTradeLifecycle,
)


OPEN_STATES = ("NEW", "PROTECTED", "TRAILING", "EXITING")


def utc_now():
    return datetime.now(timezone.utc)


def to_decimal(value):
    if value is None:
        return None
    return Decimal(str(value))


class StopPersistenceService:
    def __init__(self, session):
        self.session = session

    def build_hash(self, *, account_id, tradingsymbol, side, quantity, trigger_price, limit_price):
        payload = {
            "account_id": account_id,
            "tradingsymbol": tradingsymbol,
            "side": side,
            "quantity": int(quantity),
            "trigger_price": str(to_decimal(trigger_price)),
            "limit_price": str(to_decimal(limit_price)),
        }
        return hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()

    def get_open_lifecycle(self, *, account_id, tradingsymbol, side):
        return (
            self.session.query(StopTradeLifecycle)
            .filter(
                StopTradeLifecycle.account_id == account_id,
                StopTradeLifecycle.tradingsymbol == tradingsymbol,
                StopTradeLifecycle.side == side,
                StopTradeLifecycle.state.in_(OPEN_STATES),
            )
            .order_by(StopTradeLifecycle.opened_at.desc())
            .first()
        )

    def create_lifecycle(
        self,
        *,
        account_id,
        exchange,
        tradingsymbol,
        instrument_token=None,
        side,
        entry_qty,
        open_qty,
        entry_price,
        trade_origination_ts=None,
    ):
        now = utc_now()
        lifecycle = StopTradeLifecycle(
            account_id=account_id,
            exchange=exchange,
            tradingsymbol=tradingsymbol,
            instrument_token=instrument_token,
            side=side,
            entry_qty=int(entry_qty),
            open_qty=int(open_qty),
            entry_price=to_decimal(entry_price),
            trade_origination_ts=trade_origination_ts or now,
            state="NEW",
            opened_at=trade_origination_ts or now,
            created_at=now,
            updated_at=now,
        )
        self.session.add(lifecycle)
        self.session.flush()
        return lifecycle

    def get_latest_stop_state(self, *, lifecycle_id):
        return (
            self.session.query(StopState)
            .filter(StopState.trade_lifecycle_id == lifecycle_id)
            .one_or_none()
        )

    def upsert_stop_state(
        self,
        *,
        lifecycle_id,
        current_stop,
        trigger_price,
        limit_price,
        quantity,
        stop_type,
        last_applied_hash,
        broker_trigger_id=None,
        broker_order_status=None,
        source=None,
        calculated_at=None,
        applied_at=None,
    ):
        now = utc_now()
        state = self.get_latest_stop_state(lifecycle_id=lifecycle_id)

        if state is None:
            state = StopState(
                trade_lifecycle_id=lifecycle_id,
                created_at=now,
            )
            self.session.add(state)

        state.current_stop = to_decimal(current_stop)
        state.trigger_price = to_decimal(trigger_price)
        state.limit_price = to_decimal(limit_price)
        state.quantity = int(quantity)
        state.stop_type = stop_type
        state.last_applied_hash = last_applied_hash
        state.broker_trigger_id = broker_trigger_id
        state.broker_order_status = broker_order_status
        state.source = source
        state.calculated_at = calculated_at or now
        state.applied_at = applied_at
        state.updated_at = now

        lifecycle = self.session.query(StopTradeLifecycle).get(lifecycle_id)
        if lifecycle and lifecycle.state in ("NEW", "PROTECTED"):
            lifecycle.state = "PROTECTED" if stop_type == "INITIAL" else "TRAILING"
            lifecycle.updated_at = now

        self.session.flush()
        return state

    def insert_stop_event(
        self,
        *,
        lifecycle_id,
        event_type,
        final_stop,
        trigger_price,
        limit_price,
        quantity,
        action_taken,
        old_stop=None,
        raw_stop=None,
        validated_stop=None,
        entry_price=None,
        close_ref=None,
        atr=None,
        atr_avg=None,
        multiplier=None,
        swing_low=None,
        swing_high=None,
        source=None,
        broker_trigger_id=None,
        hash_value=None,
        reason=None,
    ):
        event = StopEvent(
            trade_lifecycle_id=lifecycle_id,
            event_type=event_type,
            old_stop=to_decimal(old_stop),
            raw_stop=to_decimal(raw_stop),
            validated_stop=to_decimal(validated_stop),
            final_stop=to_decimal(final_stop),
            trigger_price=to_decimal(trigger_price),
            limit_price=to_decimal(limit_price),
            quantity=int(quantity),
            entry_price=to_decimal(entry_price),
            close_ref=to_decimal(close_ref),
            atr=to_decimal(atr),
            atr_avg=to_decimal(atr_avg),
            multiplier=to_decimal(multiplier),
            swing_low=to_decimal(swing_low),
            swing_high=to_decimal(swing_high),
            source=source,
            action_taken=action_taken,
            broker_trigger_id=broker_trigger_id,
            hash=hash_value,
            reason=reason,
            created_at=utc_now(),
        )
        self.session.add(event)
        self.session.flush()
        return event
