from __future__ import annotations

import hashlib
import json

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


HISTORICAL_ACQUISITION_INSERT_SQL = text("""
INSERT INTO historical_price_acquisitions (
    id, source_account, target_session_date, retrieved_at, source,
    status, finality_status, content_hash
)
VALUES (
    :id, :source_account, :target_session_date, :retrieved_at,
    'ZERODHA_HISTORICAL_DATA', :status, :finality_status, :content_hash
)
""")

HISTORICAL_CHUNK_INSERT_SQL = text("""
INSERT INTO historical_price_chunks (
    id, source, symbol, exchange, tradingsymbol, instrument_token, interval,
    chunk_key, covered_from, covered_to, bar_count, content_hash
)
VALUES (
    :id, :source, :symbol, :exchange, :tradingsymbol, :instrument_token,
    :interval, :chunk_key, :covered_from, :covered_to, :bar_count, :content_hash
)
ON CONFLICT (id) DO NOTHING
""")

HISTORICAL_SNAPSHOT_INSERT_SQL = text("""
INSERT INTO historical_price_snapshots (
    id, acquisition_id, source_account, symbol, exchange, tradingsymbol,
    instrument_token, interval, target_session_date, covered_from, covered_to,
    retrieved_at, bar_count, content_hash, validation_status, finality_source,
    finality_source_key, finality_source_hash, finality_close,
    finality_retrieved_at
)
VALUES (
    :id, :acquisition_id, :source_account, :symbol, :exchange, :tradingsymbol,
    :instrument_token, :interval, :target_session_date, :covered_from, :covered_to,
    :retrieved_at, :bar_count, :content_hash, :validation_status,
    :finality_source, :finality_source_key, :finality_source_hash,
    :finality_close, :finality_retrieved_at
)
""")

HISTORICAL_SNAPSHOT_CHUNK_INSERT_SQL = text("""
INSERT INTO historical_price_snapshot_chunks (
    snapshot_id, chunk_id, ordinal, chunk_key
)
VALUES (
    :snapshot_id, :chunk_id, :ordinal, :chunk_key
)
""")

HISTORICAL_BAR_INSERT_SQL = text("""
INSERT INTO historical_price_bars (
    chunk_id, observed_at, session_date, open, high, low, close, volume, oi
)
VALUES (
    :chunk_id, :observed_at, :session_date, :open, :high, :low, :close,
    :volume, :oi
)
ON CONFLICT (chunk_id, observed_at) DO NOTHING
""")

HISTORICAL_TREND_OUTPUT_INSERT_SQL = text("""
INSERT INTO historical_trend_outputs (
    acquisition_id, daily_snapshot_id, user_id, symbol, trade_date, close,
    label, confidence, aggregate_score, internal_state, exchange,
    tradingsymbol, instrument_token
)
VALUES (
    :acquisition_id, :daily_snapshot_id, :user_id, :symbol, :trade_date, :close,
    :label, :confidence, :aggregate_score, :internal_state, :exchange,
    :tradingsymbol, :instrument_token
)
""")


def historical_snapshot_manifest_hash(references: list[dict]) -> str:
    manifest = [
        {
            "ordinal": item["ordinal"],
            "chunk_key": item["chunk_key"],
            "chunk_id": item["chunk_id"],
        }
        for item in references
    ]
    return hashlib.sha256(
        json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def validate_historical_chunk_manifests(acquisition: dict) -> None:
    """Reject incomplete or cross-identity manifests before persistence."""
    chunks = {item["id"]: item for item in acquisition["chunks"]}
    snapshots = {item["id"]: item for item in acquisition["snapshots"]}
    references: dict[str, list[dict]] = {item: [] for item in snapshots}
    for reference in acquisition["snapshot_chunks"]:
        snapshot = snapshots.get(reference["snapshot_id"])
        chunk = chunks.get(reference["chunk_id"])
        if snapshot is None or chunk is None:
            raise RuntimeError("Historical snapshot references unknown evidence.")
        identity = (
            "symbol",
            "exchange",
            "tradingsymbol",
            "instrument_token",
            "interval",
        )
        if any(snapshot[key] != chunk[key] for key in identity):
            raise RuntimeError("Historical snapshot references a cross-identity chunk.")
        if reference["chunk_key"] != chunk["chunk_key"]:
            raise RuntimeError("Historical snapshot chunk boundary is inconsistent.")
        references[reference["snapshot_id"]].append(reference)
    for snapshot_id, snapshot in snapshots.items():
        ordered = sorted(references[snapshot_id], key=lambda item: item["ordinal"])
        if [item["ordinal"] for item in ordered] != list(range(len(ordered))):
            raise RuntimeError("Historical snapshot chunk ordinals are incomplete.")
        if not ordered:
            raise RuntimeError("Historical snapshot has no chunk references.")
        if historical_snapshot_manifest_hash(ordered) != snapshot["content_hash"]:
            raise RuntimeError("Historical snapshot manifest hash is inconsistent.")


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


PREPARE_STRATEGY_RUN_SQL = text("""
INSERT INTO strategy_deterministic_engine_runs (
    run_date,
    generated_by_user_id,
    source,
    status,
    started_at,
    requested_symbols_count,
    prepared_symbols_count,
    evaluated_symbols_count,
    input_exclusion_count,
    input_exclusions_json,
    historical_acquisition_id,
    updated_at
)
VALUES (
    :run_date,
    :generated_by_user_id,
    'DB',
    'INPUTS_PREPARED',
    now(),
    :requested_symbols_count,
    :prepared_symbols_count,
    NULL,
    :input_exclusion_count,
    :input_exclusions_json,
    :historical_acquisition_id,
    now()
)
ON CONFLICT (run_date)
DO UPDATE SET
    generated_by_user_id = EXCLUDED.generated_by_user_id,
    source = 'DB',
    status = 'INPUTS_PREPARED',
    started_at = now(),
    finished_at = NULL,
    total_symbols = NULL,
    public_results_count = NULL,
    invalid_count = NULL,
    requested_symbols_count = EXCLUDED.requested_symbols_count,
    prepared_symbols_count = EXCLUDED.prepared_symbols_count,
    evaluated_symbols_count = NULL,
    input_exclusion_count = EXCLUDED.input_exclusion_count,
    input_exclusions_json = EXCLUDED.input_exclusions_json,
    historical_acquisition_id = EXCLUDED.historical_acquisition_id,
    updated_at = now()
WHERE strategy_deterministic_engine_runs.status <> 'COMPLETED'
   OR (
       :allow_unverified_completed_restart
       AND strategy_deterministic_engine_runs.requested_symbols_count IS NULL
       AND strategy_deterministic_engine_runs.prepared_symbols_count IS NULL
       AND strategy_deterministic_engine_runs.evaluated_symbols_count IS NULL
       AND strategy_deterministic_engine_runs.input_exclusion_count IS NULL
       AND strategy_deterministic_engine_runs.input_exclusions_json IS NULL
   )
RETURNING id
""")


def _persist_historical_evidence(conn, acquisition: dict, batch_size: int) -> None:
    validate_historical_chunk_manifests(acquisition)
    conn.execute(
        HISTORICAL_ACQUISITION_INSERT_SQL,
        {key: acquisition[key] for key in (
            "id", "source_account", "target_session_date", "retrieved_at",
            "status", "finality_status", "content_hash",
        )},
    )
    for start in range(0, len(acquisition["chunks"]), batch_size):
        conn.execute(
            HISTORICAL_CHUNK_INSERT_SQL,
            acquisition["chunks"][start:start + batch_size],
        )
    for start in range(0, len(acquisition["snapshots"]), batch_size):
        conn.execute(
            HISTORICAL_SNAPSHOT_INSERT_SQL,
            acquisition["snapshots"][start:start + batch_size],
        )
    for start in range(0, len(acquisition["snapshot_chunks"]), batch_size):
        conn.execute(
            HISTORICAL_SNAPSHOT_CHUNK_INSERT_SQL,
            acquisition["snapshot_chunks"][start:start + batch_size],
        )
    for start in range(0, len(acquisition["bars"]), batch_size):
        conn.execute(
            HISTORICAL_BAR_INSERT_SQL,
            acquisition["bars"][start:start + batch_size],
        )


def persist_pending_historical_acquisition(
    engine,
    *,
    acquisition: dict,
    batch_size: int = 500,
) -> None:
    """Retain retryable source evidence without linking or publishing a run."""
    if acquisition["status"] != "PENDING":
        raise ValueError("Pending historical evidence must have PENDING status.")
    with engine.begin() as conn:
        _persist_historical_evidence(conn, acquisition, batch_size)


def persist_strict_trend_preparation(
    engine,
    rows: list[dict],
    *,
    run_date,
    generated_by_user_id: str,
    requested_symbols_count: int,
    prepared_symbols_count: int,
    exclusions: list[dict],
    acquisition: dict,
    batch_size: int = 500,
    allow_unverified_completed_restart: bool = False,
) -> int:
    """Atomically persist exact-date rows and their requested-universe audit."""
    with engine.begin() as conn:
        _persist_historical_evidence(conn, acquisition, batch_size)
        for start in range(0, len(acquisition["trend_outputs"]), batch_size):
            conn.execute(
                HISTORICAL_TREND_OUTPUT_INSERT_SQL,
                acquisition["trend_outputs"][start:start + batch_size],
            )

        total = 0
        for start in range(0, len(rows), batch_size):
            batch = rows[start:start + batch_size]
            conn.execute(TREND_HISTORY_UPSERT_SQL, batch)
            total += len(batch)
        run_row = conn.execute(
            PREPARE_STRATEGY_RUN_SQL,
            {
                "run_date": run_date,
                "generated_by_user_id": generated_by_user_id,
                "requested_symbols_count": requested_symbols_count,
                "prepared_symbols_count": prepared_symbols_count,
                "input_exclusion_count": len(exclusions),
                "input_exclusions_json": json.dumps(
                    exclusions,
                    sort_keys=True,
                    separators=(",", ":"),
                ),
                "allow_unverified_completed_restart": (
                    allow_unverified_completed_restart
                ),
                "historical_acquisition_id": acquisition["id"],
            },
        ).fetchone()
        if run_row is None:
            raise RuntimeError(
                "A completed Strategy run cannot be replaced by input preparation."
            )
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
    requested_symbols_count,
    prepared_symbols_count,
    evaluated_symbols_count,
    input_exclusion_count,
    input_exclusions_json,
    historical_acquisition_id,
    updated_at
)
VALUES (
    :run_date,
    :generated_by_user_id,
    'DB',
    'STARTED',
    now(),
    :requested_symbols_count,
    :prepared_symbols_count,
    :evaluated_symbols_count,
    :input_exclusion_count,
    :input_exclusions_json,
    :historical_acquisition_id,
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
    requested_symbols_count = EXCLUDED.requested_symbols_count,
    prepared_symbols_count = EXCLUDED.prepared_symbols_count,
    evaluated_symbols_count = EXCLUDED.evaluated_symbols_count,
    input_exclusion_count = EXCLUDED.input_exclusion_count,
    input_exclusions_json = EXCLUDED.input_exclusions_json,
    historical_acquisition_id = EXCLUDED.historical_acquisition_id,
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

DELETE_STRATEGY_BATCH_RESULTS_FOR_DATE_SQL = text("""
DELETE FROM strategy_deterministic_engine_batch_results
WHERE run_date = :run_date
""")


def create_or_restart_strategy_run(
    conn,
    run_date: date,
    generated_by_user_id: str | None,
    input_provenance: dict | None = None,
) -> int:
    provenance = input_provenance or {}
    exclusions = provenance.get("input_exclusions")
    row = conn.execute(
        CREATE_OR_UPDATE_STRATEGY_RUN_SQL,
        {
            "run_date": run_date,
            "generated_by_user_id": generated_by_user_id,
            "requested_symbols_count": provenance.get("requested_symbols_count"),
            "prepared_symbols_count": provenance.get("prepared_symbols_count"),
            "evaluated_symbols_count": provenance.get("evaluated_symbols_count"),
            "input_exclusion_count": (
                len(exclusions) if exclusions is not None else None
            ),
            "input_exclusions_json": (
                json.dumps(exclusions, sort_keys=True, separators=(",", ":"))
                if exclusions is not None
                else None
            ),
            "historical_acquisition_id": provenance.get(
                "historical_acquisition_id"
            ),
        },
    ).fetchone()
    return int(row[0])


def clear_strategy_batch_results_for_run_date(conn, run_date: date) -> int:
    result = conn.execute(
        DELETE_STRATEGY_BATCH_RESULTS_FOR_DATE_SQL,
        {"run_date": run_date},
    )
    return int(result.rowcount or 0)


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
