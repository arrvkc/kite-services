from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import date
from decimal import Decimal, InvalidOperation
import json

from sqlalchemy import text

from engines.strategy_deterministic_engine.adapters.trend_identifier_db_adapter import (
    TrendIdentifierDbAdapter,
)
from engines.strategy_deterministic_engine.db.postgres import get_engine


NO_TRADING_ACTIVITY = "NO_TRADING_ACTIVITY"


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


def validate_strategy_input_coverage(
    strategy_input_count: int,
    target_symbols: set[str],
    excluded_symbols: set[str],
) -> None:
    if not excluded_symbols.issubset(target_symbols):
        raise RuntimeError("Strategy input exclusions are outside the target universe.")
    if strategy_input_count + len(excluded_symbols) != len(target_symbols):
        raise RuntimeError("One or more Strategy inputs were silently excluded.")


def _zero_decimal(value) -> bool:
    try:
        return Decimal(str(value)) == 0
    except (InvalidOperation, TypeError, ValueError):
        return False


def validate_no_trading_exclusion(exclusion: dict, run_date: date) -> None:
    if exclusion.get("reason") != NO_TRADING_ACTIVITY:
        raise RuntimeError("Input preparation contains an unsupported exclusion reason.")
    if exclusion.get("stage") != "TREND_PREPARATION":
        raise RuntimeError("No-trading exclusion has an invalid preparation stage.")
    if exclusion.get("target_date") != run_date.isoformat():
        raise RuntimeError("No-trading exclusion target date does not match the run date.")
    if not str(exclusion.get("symbol") or "").strip():
        raise RuntimeError("No-trading exclusion is missing its logical symbol.")
    instrument = exclusion.get("selected_instrument")
    if not isinstance(instrument, dict) or not all(
        instrument.get(key) not in (None, "")
        for key in ("exchange", "tradingsymbol", "instrument_token")
    ):
        raise RuntimeError("No-trading exclusion is missing selected instrument context.")
    evidence = exclusion.get("evidence")
    if not isinstance(evidence, dict):
        raise RuntimeError("No-trading exclusion is missing evidence.")
    if (
        not evidence.get("daily_timestamp")
        or not _zero_decimal(evidence.get("daily_volume"))
        or evidence.get("required_interval") != "60minute"
        or evidence.get("target_intraday_candle_count") != 0
        or evidence.get("market_session_confirmation")
        != "PREPARED_PEER_TARGET_SESSION"
    ):
        raise RuntimeError("No-trading exclusion evidence is incomplete or ambiguous.")


def load_input_preparation_manifest(connection, run_date: date) -> dict:
    row = connection.execute(
        text(
            "SELECT status, requested_symbols_count, prepared_symbols_count, "
            "       evaluated_symbols_count, input_exclusion_count, "
            "       input_exclusions_json "
            "FROM strategy_deterministic_engine_runs WHERE run_date = :run_date"
        ),
        {"run_date": run_date},
    ).mappings().one_or_none()
    if row is None:
        raise RuntimeError("Exact-date Strategy input preparation audit is missing.")
    if row["status"] not in {"INPUTS_PREPARED", "STARTED", "COMPLETED"}:
        raise RuntimeError("Exact-date Strategy input preparation status is invalid.")
    try:
        exclusions = json.loads(row["input_exclusions_json"] or "[]")
    except (TypeError, ValueError) as exc:
        raise RuntimeError("Strategy input exclusions are not valid JSON.") from exc
    if not isinstance(exclusions, list):
        raise RuntimeError("Strategy input exclusions must be a list.")
    symbols = [str(item.get("symbol") or "").strip().upper() for item in exclusions]
    if len(symbols) != len(set(symbols)):
        raise RuntimeError("Strategy input exclusions contain duplicate symbols.")
    preparation_exclusions = []
    strategy_input_exclusions = []
    for exclusion in exclusions:
        if not isinstance(exclusion, dict):
            raise RuntimeError("Strategy input exclusion is malformed.")
        if exclusion.get("stage") == "TREND_PREPARATION":
            validate_no_trading_exclusion(exclusion, run_date)
            preparation_exclusions.append(exclusion)
        elif (
            exclusion.get("stage") == "STRATEGY_INPUT"
            and exclusion.get("reason") == "NULL_AGGREGATE_SCORE"
            and str(exclusion.get("symbol") or "").strip()
        ):
            strategy_input_exclusions.append(exclusion)
        else:
            raise RuntimeError("Input preparation contains an unsupported exclusion reason.")
    if row["requested_symbols_count"] is None or row["prepared_symbols_count"] is None:
        raise RuntimeError("Strategy input preparation counts are missing.")
    if int(row["input_exclusion_count"] or 0) != len(exclusions):
        raise RuntimeError("Strategy input exclusion count does not match its audit.")
    if int(row["requested_symbols_count"]) != int(row["prepared_symbols_count"]) + len(
        preparation_exclusions
    ):
        raise RuntimeError("Requested and prepared Strategy counts do not reconcile.")
    if row["evaluated_symbols_count"] is None:
        if strategy_input_exclusions:
            raise RuntimeError("Strategy input exclusions exist before evaluation.")
    elif int(row["prepared_symbols_count"]) != int(
        row["evaluated_symbols_count"]
    ) + len(strategy_input_exclusions):
        raise RuntimeError("Prepared and evaluated Strategy counts do not reconcile.")
    return {
        "requested_symbols_count": int(row["requested_symbols_count"]),
        "prepared_symbols_count": int(row["prepared_symbols_count"]),
        "exclusions": preparation_exclusions,
        "stored_strategy_input_exclusions": strategy_input_exclusions,
    }


def validate_preparation_symbol_coverage(
    trend_symbols: set[str],
    contract_symbols: set[str],
    no_trading_symbols: set[str],
    *,
    requested_symbols_count: int,
    prepared_symbols_count: int,
) -> None:
    if trend_symbols & no_trading_symbols:
        raise RuntimeError("A no-trading exclusion also has a target-date Trend row.")
    if trend_symbols | no_trading_symbols != contract_symbols:
        raise RuntimeError(
            "Exact target-date Trend, exclusion, and contract symbol sets differ."
        )
    if prepared_symbols_count != len(trend_symbols):
        raise RuntimeError("Prepared Strategy count does not match target Trend rows.")
    if requested_symbols_count != len(contract_symbols):
        raise RuntimeError("Requested Strategy count does not match contract universe.")


def verify_inputs(engine, run_date: date, history_days: int = 5) -> dict:
    with engine.connect() as connection:
        preparation = load_input_preparation_manifest(connection, run_date)
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
        null_score_symbols = {
            row[0]
            for row in connection.execute(
                text(
                    "WITH target_symbols AS ("
                    "  SELECT DISTINCT symbol FROM trend_history_fo_universe "
                    "  WHERE trade_date = :run_date"
                    "), ranked AS ("
                    "  SELECT history.symbol, history.aggregate_score, "
                    "         ROW_NUMBER() OVER ("
                    "           PARTITION BY history.symbol "
                    "           ORDER BY history.trade_date DESC"
                    "         ) AS session_rank "
                    "  FROM trend_history_fo_universe AS history "
                    "  JOIN target_symbols USING (symbol) "
                    "  WHERE history.trade_date <= :run_date"
                    ") "
                    "SELECT symbol FROM ranked "
                    "WHERE session_rank <= :history_days "
                    "GROUP BY symbol "
                    "HAVING COUNT(*) FILTER (WHERE aggregate_score IS NULL) > 0"
                ),
                {"run_date": run_date, "history_days": history_days},
            )
        }
        contract_row_count = connection.execute(
            text(
                "SELECT COUNT(*) FROM contract_snapshot_fo_universe "
                "WHERE selection_date = :run_date"
            ),
            {"run_date": run_date},
        ).scalar_one()

    if not trend_symbols:
        raise RuntimeError("Exact target-date Trend History has no symbols.")
    no_trading_symbols = {
        str(item["symbol"]).upper() for item in preparation["exclusions"]
    }
    validate_preparation_symbol_coverage(
        trend_symbols,
        contract_symbols,
        no_trading_symbols,
        requested_symbols_count=preparation["requested_symbols_count"],
        prepared_symbols_count=preparation["prepared_symbols_count"],
    )
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
    validate_strategy_input_coverage(
        len(strategy_inputs),
        trend_symbols,
        null_score_symbols,
    )

    null_score_exclusions = [
        {"symbol": symbol, "reason": "NULL_AGGREGATE_SCORE", "stage": "STRATEGY_INPUT"}
        for symbol in sorted(null_score_symbols)
    ]
    if preparation["stored_strategy_input_exclusions"] and (
        preparation["stored_strategy_input_exclusions"] != null_score_exclusions
    ):
        raise RuntimeError("Persisted Strategy input exclusions changed after evaluation.")
    all_exclusions = preparation["exclusions"] + null_score_exclusions

    return {
        "run_date": run_date.isoformat(),
        "trend_dates": trend_date_patterns[0]["dates"],
        "trend_date_patterns": trend_date_patterns,
        "trend_symbol_count": len(trend_symbols),
        "contract_symbol_count": len(contract_symbols),
        "requested_symbols_count": preparation["requested_symbols_count"],
        "prepared_symbols_count": preparation["prepared_symbols_count"],
        "strategy_input_count": len(strategy_inputs),
        "strategy_input_exclusion_count": len(all_exclusions),
        "strategy_input_exclusions": all_exclusions,
        "no_trading_activity_count": len(no_trading_symbols),
        "no_trading_activity_symbols": sorted(no_trading_symbols),
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
