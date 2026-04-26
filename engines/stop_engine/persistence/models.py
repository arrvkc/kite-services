from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    Numeric,
    String,
    Text,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship


Base = declarative_base()


class StopTradeLifecycle(Base):
    __tablename__ = "stop_trade_lifecycles"

    id = Column(BigInteger, primary_key=True)
    account_id = Column(String(64), nullable=False)
    exchange = Column(String(32), nullable=False)
    tradingsymbol = Column(String(128), nullable=False)
    instrument_token = Column(BigInteger)
    side = Column(String(16), nullable=False)
    entry_qty = Column(Integer, nullable=False)
    open_qty = Column(Integer, nullable=False)
    entry_price = Column(Numeric(18, 4), nullable=False)
    trade_origination_ts = Column(DateTime(timezone=True), nullable=False)
    state = Column(String(32), nullable=False)
    opened_at = Column(DateTime(timezone=True), nullable=False)
    closed_at = Column(DateTime(timezone=True))
    created_at = Column(DateTime(timezone=True), nullable=False)
    updated_at = Column(DateTime(timezone=True), nullable=False)

    stop_state = relationship("StopState", uselist=False, back_populates="lifecycle")
    events = relationship("StopEvent", back_populates="lifecycle")


class StopState(Base):
    __tablename__ = "stop_states"

    id = Column(BigInteger, primary_key=True)
    trade_lifecycle_id = Column(BigInteger, ForeignKey("stop_trade_lifecycles.id"), nullable=False)
    current_stop = Column(Numeric(18, 4), nullable=False)
    trigger_price = Column(Numeric(18, 4), nullable=False)
    limit_price = Column(Numeric(18, 4), nullable=False)
    quantity = Column(Integer, nullable=False)
    stop_type = Column(String(32), nullable=False)
    last_applied_hash = Column(String(128), nullable=False)
    broker_trigger_id = Column(String(128))
    broker_order_status = Column(String(64))
    source = Column(String(64))
    calculated_at = Column(DateTime(timezone=True), nullable=False)
    applied_at = Column(DateTime(timezone=True))
    created_at = Column(DateTime(timezone=True), nullable=False)
    updated_at = Column(DateTime(timezone=True), nullable=False)

    lifecycle = relationship("StopTradeLifecycle", back_populates="stop_state")


class StopEvent(Base):
    __tablename__ = "stop_events"

    id = Column(BigInteger, primary_key=True)
    trade_lifecycle_id = Column(BigInteger, ForeignKey("stop_trade_lifecycles.id"), nullable=False)
    event_type = Column(String(64), nullable=False)
    old_stop = Column(Numeric(18, 4))
    raw_stop = Column(Numeric(18, 4))
    validated_stop = Column(Numeric(18, 4))
    final_stop = Column(Numeric(18, 4), nullable=False)
    trigger_price = Column(Numeric(18, 4), nullable=False)
    limit_price = Column(Numeric(18, 4), nullable=False)
    quantity = Column(Integer, nullable=False)
    entry_price = Column(Numeric(18, 4))
    close_ref = Column(Numeric(18, 4))
    atr = Column(Numeric(18, 4))
    atr_avg = Column(Numeric(18, 4))
    multiplier = Column(Numeric(10, 4))
    swing_low = Column(Numeric(18, 4))
    swing_high = Column(Numeric(18, 4))
    source = Column(String(64))
    action_taken = Column(String(64), nullable=False)
    broker_trigger_id = Column(String(128))
    hash = Column(String(128))
    reason = Column(Text)
    created_at = Column(DateTime(timezone=True), nullable=False)

    lifecycle = relationship("StopTradeLifecycle", back_populates="events")
