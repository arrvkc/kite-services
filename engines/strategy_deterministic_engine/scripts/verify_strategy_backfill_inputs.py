from __future__ import annotations

import argparse
from datetime import date
import json

from sqlalchemy import text

from engines.strategy_deterministic_engine.adapters.trend_identifier_db_adapter import (
    TrendIdentifierDbAdapter,
)
from engines.strategy_deterministic_engine.db.postgres import get_engine


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Verify exact-date Strategy recovery inputs without writing data."
    )
    parser.add_argument("--run-date", required=True)
    parser.add_argument("--history-days", type=int, default=5)
    return parser


def verify_inputs(engine, run_date: date, history_days: int = 5) -> dict:
    with engine.connect() as connection:
        trend_dates = [
            row[0]
            for row in connection.execute(
                text(
                    "SELECT DISTINCT trade_date FROM trend_history_fo_universe "
                    "WHERE trade_date <= :run_date ORDER BY trade_date DESC "
                    "LIMIT :history_days"
                ),
                {"run_date": run_date, "history_days": history_days},
            )
        ]
        trend_symbols = {
            row[0]
            for row in connection.execute(
                text(
                    "SELECT DISTINCT symbol FROM trend_history_fo_universe "
                    "WHERE trade_date = :run_date"
                ),
                {"run_date": run_date},
            )
        }
        contract_symbols = {
            row[0]
            for row in connection.execute(
                text(
                    "SELECT DISTINCT symbol FROM contract_snapshot_fo_universe "
                    "WHERE selection_date = :run_date"
                ),
                {"run_date": run_date},
            )
        }
        incomplete_trend_symbols = connection.execute(
            text(
                "SELECT symbol FROM trend_history_fo_universe "
                "WHERE trade_date IN ("
                "SELECT DISTINCT trade_date FROM trend_history_fo_universe "
                "WHERE trade_date <= :run_date ORDER BY trade_date DESC "
                "LIMIT :history_days) AND symbol IN ("
                "SELECT DISTINCT symbol FROM trend_history_fo_universe "
                "WHERE trade_date = :run_date) "
                "GROUP BY symbol HAVING COUNT(*) <> :history_days "
                "OR COUNT(DISTINCT trade_date) <> :history_days"
            ),
            {"run_date": run_date, "history_days": history_days},
        ).fetchall()
        contract_row_count = connection.execute(
            text(
                "SELECT COUNT(*) FROM contract_snapshot_fo_universe "
                "WHERE selection_date = :run_date"
            ),
            {"run_date": run_date},
        ).scalar_one()

    ordered_dates = sorted(trend_dates)
    if len(ordered_dates) != history_days or ordered_dates[-1] != run_date:
        raise RuntimeError("Exact target-date Trend History is incomplete.")
    if not trend_symbols:
        raise RuntimeError("Exact target-date Trend History has no symbols.")
    if trend_symbols != contract_symbols:
        raise RuntimeError("Exact target-date Trend and contract symbol sets differ.")
    if incomplete_trend_symbols:
        raise RuntimeError("Trend History contains incomplete or duplicate symbol sessions.")
    if int(contract_row_count) != len(contract_symbols):
        raise RuntimeError("Contract snapshot contains duplicate target-date symbol rows.")

    strategy_inputs = TrendIdentifierDbAdapter(
        engine=engine,
        run_date=run_date,
        history_days=history_days,
        require_exact_contract_snapshot=True,
    ).build_all()
    if not strategy_inputs:
        raise RuntimeError("No exact-date Strategy inputs were produced.")
    if len(strategy_inputs) != len(trend_symbols):
        raise RuntimeError("One or more exact-date symbols lacked five coherent Trend sessions.")

    return {
        "run_date": run_date.isoformat(),
        "trend_dates": [item.isoformat() for item in ordered_dates],
        "trend_symbol_count": len(trend_symbols),
        "contract_symbol_count": len(contract_symbols),
        "strategy_input_count": len(strategy_inputs),
        "contract_snapshot_exact": True,
        "future_trend_rows_selected": 0,
    }


def main() -> None:
    args = build_argument_parser().parse_args()
    run_date = date.fromisoformat(args.run_date)
    payload = verify_inputs(get_engine(), run_date, args.history_days)
    print(json.dumps(payload, sort_keys=True, separators=(",", ":")))


if __name__ == "__main__":
    main()
