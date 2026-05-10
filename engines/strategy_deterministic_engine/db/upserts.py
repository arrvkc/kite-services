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


from datetime import date
from sqlalchemy import text


CREATE_OR_UPDATE_STRATEGY_RUN_SQL = text("""
INSERT INTO strategy_deterministic_engine_runs (
    run_date,
    generated_by_user_id,
    source,
    status,
    started_at,
    updated_at
)
VALUES (
    :run_date,
    :generated_by_user_id,
    'DB',
    'STARTED',
    now(),
    now()
)
ON CONFLICT (run_date)
DO UPDATE SET
    generated_by_user_id = EXCLUDED.generated_by_user_id,
    source = 'DB',
    status = 'STARTED',
    started_at = now(),
    finished_at = NULL,
    total_symbols = NULL,
    public_results_count = NULL,
    invalid_count = NULL,
    updated_at = now()
RETURNING id
""")


COMPLETE_STRATEGY_RUN_SQL = text("""
UPDATE strategy_deterministic_engine_runs
SET
    status = :status,
    finished_at = now(),
    total_symbols = :total_symbols,
    public_results_count = :public_results_count,
    invalid_count = :invalid_count,
    updated_at = now()
WHERE id = :run_id
""")


STRATEGY_BATCH_RESULT_UPSERT_SQL = text("""
INSERT INTO strategy_deterministic_engine_batch_results (
    run_id,
    run_date,
    symbol,
    label,
    score,
    confidence,
    state,
    bull_count_5,
    bear_count_5,
    flat_count_5,
    sign_flip_count_5,
    mean_score_3,
    dte_near_month,
    dte_next_month,
    regime_bucket,
    candidate_family,
    strategy_family,
    contract_month_selection,
    final_strategy_strength,
    include_in_top_n,
    rank_overall,
    rank_in_family,
    strategy_transition_state,
    reason_codes,
    updated_at
)
VALUES (
    :run_id,
    :run_date,
    :symbol,
    :label,
    :score,
    :confidence,
    :state,
    :bull_count_5,
    :bear_count_5,
    :flat_count_5,
    :sign_flip_count_5,
    :mean_score_3,
    :dte_near_month,
    :dte_next_month,
    :regime_bucket,
    :candidate_family,
    :strategy_family,
    :contract_month_selection,
    :final_strategy_strength,
    :include_in_top_n,
    :rank_overall,
    :rank_in_family,
    :strategy_transition_state,
    :reason_codes,
    now()
)
ON CONFLICT (run_date, symbol)
DO UPDATE SET
    run_id = EXCLUDED.run_id,
    label = EXCLUDED.label,
    score = EXCLUDED.score,
    confidence = EXCLUDED.confidence,
    state = EXCLUDED.state,
    bull_count_5 = EXCLUDED.bull_count_5,
    bear_count_5 = EXCLUDED.bear_count_5,
    flat_count_5 = EXCLUDED.flat_count_5,
    sign_flip_count_5 = EXCLUDED.sign_flip_count_5,
    mean_score_3 = EXCLUDED.mean_score_3,
    dte_near_month = EXCLUDED.dte_near_month,
    dte_next_month = EXCLUDED.dte_next_month,
    regime_bucket = EXCLUDED.regime_bucket,
    candidate_family = EXCLUDED.candidate_family,
    strategy_family = EXCLUDED.strategy_family,
    contract_month_selection = EXCLUDED.contract_month_selection,
    final_strategy_strength = EXCLUDED.final_strategy_strength,
    include_in_top_n = EXCLUDED.include_in_top_n,
    rank_overall = EXCLUDED.rank_overall,
    rank_in_family = EXCLUDED.rank_in_family,
    strategy_transition_state = EXCLUDED.strategy_transition_state,
    reason_codes = EXCLUDED.reason_codes,
    updated_at = now()
""")


def create_or_restart_strategy_run(conn, run_date: date, generated_by_user_id: str | None) -> int:
    row = conn.execute(
        CREATE_OR_UPDATE_STRATEGY_RUN_SQL,
        {
            "run_date": run_date,
            "generated_by_user_id": generated_by_user_id,
        },
    ).fetchone()
    return int(row[0])


def complete_strategy_run(
    conn,
    run_id: int,
    status: str,
    total_symbols: int,
    public_results_count: int,
    invalid_count: int,
) -> None:
    conn.execute(
        COMPLETE_STRATEGY_RUN_SQL,
        {
            "run_id": run_id,
            "status": status,
            "total_symbols": total_symbols,
            "public_results_count": public_results_count,
            "invalid_count": invalid_count,
        },
    )


def upsert_strategy_batch_result_rows(conn, rows: list[dict], batch_size: int = 500) -> int:
    if not rows:
        return 0

    total = 0
    for start in range(0, len(rows), batch_size):
        batch = rows[start:start + batch_size]
        conn.execute(STRATEGY_BATCH_RESULT_UPSERT_SQL, batch)
        total += len(batch)
    return total
