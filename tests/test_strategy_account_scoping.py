from __future__ import annotations

from datetime import date
import sqlite3

import pytest
from sqlalchemy import create_engine, text

from engines.strategy_deterministic_engine.scripts.verify_strategy_backfill_inputs import (
    verify_inputs,
)


RUN_DATE = date(2026, 8, 20)
SESSION_DAYS = (14, 17, 18, 19, 20)


def _engine():
    engine = create_engine(
        "sqlite://",
        connect_args={
            "detect_types": sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
        },
        native_datetime=True,
    )
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE trend_history_fo_universe (
                user_id TEXT NOT NULL, symbol TEXT NOT NULL, trade_date DATE NOT NULL,
                close NUMERIC, label TEXT, confidence NUMERIC,
                aggregate_score NUMERIC, internal_state TEXT, exchange TEXT,
                tradingsymbol TEXT, instrument_token INTEGER
            )
        """))
        connection.execute(text("""
            CREATE TABLE contract_snapshot_fo_universe (
                user_id TEXT NOT NULL, symbol TEXT NOT NULL, selection_date DATE NOT NULL,
                near_expiry DATE, next_expiry DATE, dte_near_month INTEGER,
                next_month_available BOOLEAN, dte_next_month INTEGER
            )
        """))
        connection.execute(text("""
            CREATE TABLE strategy_deterministic_engine_runs (
                run_date DATE PRIMARY KEY, status TEXT NOT NULL,
                generated_by_user_id TEXT, requested_symbols_count INTEGER,
                prepared_symbols_count INTEGER, evaluated_symbols_count INTEGER,
                input_exclusion_count INTEGER, input_exclusions_json TEXT
            )
        """))
    return engine


def _seed_account(connection, user_id: str, days=SESSION_DAYS, close_base=100):
    for day in days:
        connection.execute(
            text("""
                INSERT INTO trend_history_fo_universe VALUES
                (:user_id, 'ABC', :trade_date, :close, 'UP', 0.8,
                 0.5, 'TRENDING', 'NSE', 'ABC', 1)
            """),
            {
                "user_id": user_id,
                "trade_date": f"2026-08-{day:02d}",
                "close": close_base + day,
            },
        )
    connection.execute(
        text("""
            INSERT INTO contract_snapshot_fo_universe VALUES
            (:user_id, 'ABC', '2026-08-20', '2026-08-27',
             '2026-09-29', 5, 1, 27)
        """),
        {"user_id": user_id},
    )


def _seed_run(connection, user_id: str):
    connection.execute(
        text("""
            INSERT INTO strategy_deterministic_engine_runs VALUES
            ('2026-08-20', 'INPUTS_PREPARED', :user_id, 1, 1, NULL, 0, '[]')
        """),
        {"user_id": user_id},
    )


def test_parallel_history_uses_only_authoritative_account_without_mutation():
    engine = _engine()
    with engine.begin() as connection:
        _seed_account(connection, "OMK569", close_base=100)
        _seed_account(connection, "XJ1877", close_base=200)
        _seed_run(connection, "XJ1877")

    before = {}
    with engine.connect() as connection:
        for user_id in ("OMK569", "XJ1877"):
            before[user_id] = connection.execute(
                text("SELECT COUNT(*) FROM trend_history_fo_universe WHERE user_id=:user_id"),
                {"user_id": user_id},
            ).scalar_one()

    result = verify_inputs(engine, RUN_DATE, "XJ1877", history_days=5)

    assert result["user_id"] == "XJ1877"
    assert result["trend_dates"] == [
        "2026-08-14",
        "2026-08-17",
        "2026-08-18",
        "2026-08-19",
        "2026-08-20",
    ]
    assert result["strategy_input_count"] == 1
    with engine.connect() as connection:
        after = {
            user_id: connection.execute(
                text("SELECT COUNT(*) FROM trend_history_fo_universe WHERE user_id=:user_id"),
                {"user_id": user_id},
            ).scalar_one()
            for user_id in ("OMK569", "XJ1877")
        }
    assert after == before


def test_requested_account_missing_target_fails_despite_other_account_history():
    engine = _engine()
    with engine.begin() as connection:
        _seed_account(connection, "OMK569")
        _seed_account(connection, "XJ1877", days=(13, 14, 17, 18, 19))
        _seed_run(connection, "XJ1877")

    with pytest.raises(RuntimeError, match="has no symbols"):
        verify_inputs(engine, RUN_DATE, "XJ1877", history_days=5)


def test_explicit_historical_account_retains_its_own_history():
    engine = _engine()
    with engine.begin() as connection:
        _seed_account(connection, "OMK569")
        _seed_account(connection, "XJ1877", close_base=200)
        _seed_run(connection, "OMK569")

    result = verify_inputs(engine, RUN_DATE, "OMK569", history_days=5)

    assert result["user_id"] == "OMK569"
    assert result["strategy_input_count"] == 1


def test_run_provenance_must_match_requested_account():
    engine = _engine()
    with engine.begin() as connection:
        _seed_account(connection, "XJ1877")
        _seed_run(connection, "OMK569")

    with pytest.raises(RuntimeError, match="belongs to another account"):
        verify_inputs(engine, RUN_DATE, "XJ1877", history_days=5)
