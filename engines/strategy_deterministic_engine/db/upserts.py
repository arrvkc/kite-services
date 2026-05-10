from __future__ import annotations

from sqlalchemy import text


TREND_HISTORY_UPSERT_SQL = text("""
INSERT INTO trend_history_fo_universe (
    user_id,
    symbol,
    trade_date,
    close,
    label,
    confidence,
    aggregate_score,
    internal_state,
    exchange,
    tradingsymbol,
    instrument_token,
    updated_at
)
VALUES (
    :user_id,
    :symbol,
    :trade_date,
    :close,
    :label,
    :confidence,
    :aggregate_score,
    :internal_state,
    :exchange,
    :tradingsymbol,
    :instrument_token,
    now()
)
ON CONFLICT (user_id, symbol, trade_date)
DO UPDATE SET
    close = EXCLUDED.close,
    label = EXCLUDED.label,
    confidence = EXCLUDED.confidence,
    aggregate_score = EXCLUDED.aggregate_score,
    internal_state = EXCLUDED.internal_state,
    exchange = EXCLUDED.exchange,
    tradingsymbol = EXCLUDED.tradingsymbol,
    instrument_token = EXCLUDED.instrument_token,
    updated_at = now()
""")


CONTRACT_SNAPSHOT_UPSERT_SQL = text("""
INSERT INTO contract_snapshot_fo_universe (
    user_id,
    symbol,
    selection_date,
    near_expiry,
    next_expiry,
    dte_near_month,
    next_month_available,
    dte_next_month,
    updated_at
)
VALUES (
    :user_id,
    :symbol,
    :selection_date,
    :near_expiry,
    :next_expiry,
    :dte_near_month,
    :next_month_available,
    :dte_next_month,
    now()
)
ON CONFLICT (user_id, symbol, selection_date)
DO UPDATE SET
    near_expiry = EXCLUDED.near_expiry,
    next_expiry = EXCLUDED.next_expiry,
    dte_near_month = EXCLUDED.dte_near_month,
    next_month_available = EXCLUDED.next_month_available,
    dte_next_month = EXCLUDED.dte_next_month,
    updated_at = now()
""")


def upsert_trend_history_rows(engine, rows: list[dict], batch_size: int = 500) -> int:
    if not rows:
        return 0

    total = 0
    with engine.begin() as conn:
        for start in range(0, len(rows), batch_size):
            batch = rows[start:start + batch_size]
            conn.execute(TREND_HISTORY_UPSERT_SQL, batch)
            total += len(batch)
    return total


def upsert_contract_snapshot_rows(engine, rows: list[dict], batch_size: int = 500) -> int:
    if not rows:
        return 0

    total = 0
    with engine.begin() as conn:
        for start in range(0, len(rows), batch_size):
            batch = rows[start:start + batch_size]
            conn.execute(CONTRACT_SNAPSHOT_UPSERT_SQL, batch)
            total += len(batch)
    return total
