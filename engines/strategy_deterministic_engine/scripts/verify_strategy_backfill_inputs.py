from __future__ import annotations

import argparse
from collections import Counter, defaultdict
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


def build_trend_date_patterns(rows, run_date: date, history_days: int) -> list[dict]:
    """Validate and summarize the exact per-symbol sessions used by the adapter."""
    dates_by_symbol: dict[str, list[date]] = defaultdict(list)
    for symbol, trade_date in rows:
        dates_by_symbol[str(symbol)].append(trade_date)

    invalid_symbols = []
    pattern_counts: Counter[tuple[date, ...]] = Counter()
    for symbol, trade_dates in dates_by_symbol.items():
        ordered_dates = tuple(sorted(trade_dates))
        if (
            len(ordered_dates) != history_days
            or len(set(ordered_dates)) != history_days
            or ordered_dates[-1] != run_date
            or any(item > run_date for item in ordered_dates)
        ):
            invalid_symbols.append(symbol)
            continue
        pattern_counts[ordered_dates] += 1

    if invalid_symbols:
        raise RuntimeError(
            "Trend History contains incomplete, duplicate, or non-target sessions."
        )
    if not pattern_counts:
        raise RuntimeError("Exact target-date Trend History has no session patterns.")

    return [
        {
            "dates": [item.isoformat() for item in pattern],
            "symbol_count": symbol_count,
        }
        for pattern, symbol_count in sorted(
            pattern_counts.items(),
            key=lambda item: (-item[1], item[0]),
        )
    ]


def verify_inputs(engine, run_date: date, history_days: int = 5) -> dict:
    with engine.connect() as connection:
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
        selected_trend_rows = connection.execute(
            text(
                "WITH target_symbols AS ("
                "  SELECT DISTINCT symbol FROM trend_history_fo_universe "
                "  WHERE trade_date = :run_date"
                "), ranked AS ("
                "  SELECT history.symbol, history.trade_date, "
                "         ROW_NUMBER() OVER ("
                "           PARTITION BY history.symbol "
                "           ORDER BY history.trade_date DESC"
                "         ) AS session_rank "
                "  FROM trend_history_fo_universe AS history "
                "  JOIN target_symbols USING (symbol) "
                "  WHERE history.trade_date <= :run_date"
                ") "
                "SELECT symbol, trade_date FROM ranked "
                "WHERE session_rank <= :history_days "
                "ORDER BY symbol, trade_date"
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

    if not trend_symbols:
        raise RuntimeError("Exact target-date Trend History has no symbols.")
    if trend_symbols != contract_symbols:
        raise RuntimeError("Exact target-date Trend and contract symbol sets differ.")
    trend_date_patterns = build_trend_date_patterns(
        selected_trend_rows,
        run_date,
        history_days,
    )
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
        "trend_dates": trend_date_patterns[0]["dates"],
        "trend_date_patterns": trend_date_patterns,
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
