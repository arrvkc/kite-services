from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
import json
from zoneinfo import ZoneInfo

from sqlalchemy import text

from engines.strategy_deterministic_engine.adapters.trend_identifier_db_adapter import (
    TrendIdentifierDbAdapter,
)
from engines.strategy_deterministic_engine.db.postgres import get_engine
from engines.trend_identifier.trend_identifier.runners.equity_trend_history_runner import (
    GOVERNED_TARGET_DATE_UNAVAILABLE_SYMBOLS,
    TARGET_DATE_HISTORY_UNAVAILABLE,
)


NO_TRADING_ACTIVITY = "NO_TRADING_ACTIVITY"


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Verify exact-date Strategy recovery inputs without writing data."
    )
    parser.add_argument("--run-date", required=True)
    parser.add_argument("--user-id", required=True)
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


def validate_target_date_unavailable_exclusion(
    exclusion: dict, run_date: date
) -> None:
    if exclusion.get("reason") != TARGET_DATE_HISTORY_UNAVAILABLE:
        raise RuntimeError("Input preparation contains an unsupported exclusion reason.")
    if exclusion.get("stage") != "TREND_PREPARATION":
        raise RuntimeError(
            "Target-date-unavailable exclusion has an invalid preparation stage."
        )
    if exclusion.get("target_date") != run_date.isoformat():
        raise RuntimeError(
            "Target-date-unavailable exclusion date does not match the run date."
        )
    symbol = str(exclusion.get("symbol") or "").strip().upper()
    if symbol not in GOVERNED_TARGET_DATE_UNAVAILABLE_SYMBOLS:
        raise RuntimeError(
            "Target-date-unavailable exclusion is not governed for this symbol."
        )
    instrument = exclusion.get("selected_instrument")
    if not isinstance(instrument, dict) or not all(
        instrument.get(key) not in (None, "")
        for key in ("exchange", "tradingsymbol", "instrument_token")
    ):
        raise RuntimeError(
            "Target-date-unavailable exclusion is missing selected instrument context."
        )
    evidence = exclusion.get("evidence")
    if not isinstance(evidence, dict):
        raise RuntimeError("Target-date-unavailable exclusion is missing evidence.")
    try:
        latest_daily = datetime.fromisoformat(
            str(evidence["latest_daily_timestamp"]).replace("Z", "+00:00")
        ).astimezone(ZoneInfo("Asia/Kolkata")).date()
        latest_intraday = datetime.fromisoformat(
            str(evidence["latest_intraday_timestamp"]).replace("Z", "+00:00")
        ).astimezone(ZoneInfo("Asia/Kolkata")).date()
    except (KeyError, TypeError, ValueError) as exc:
        raise RuntimeError(
            "Target-date-unavailable exclusion has invalid prior-source evidence."
        ) from exc
    if (
        evidence.get("required_interval") != "60minute"
        or evidence.get("target_daily_candle_count") != 0
        or evidence.get("target_intraday_candle_count") != 0
        or latest_daily >= run_date
        or latest_intraday >= run_date
        or evidence.get("market_session_confirmation")
        != "PREPARED_PEER_TARGET_SESSION"
    ):
        raise RuntimeError(
            "Target-date-unavailable exclusion evidence is incomplete or ambiguous."
        )


def load_input_preparation_manifest(connection, run_date: date, user_id: str) -> dict:
    row = connection.execute(
        text(
            "SELECT status, generated_by_user_id, requested_symbols_count, prepared_symbols_count, "
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
    if str(row["generated_by_user_id"] or "").strip().upper() != user_id:
        raise RuntimeError(
            "Exact-date Strategy input preparation belongs to another account."
        )
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
            if exclusion.get("reason") == NO_TRADING_ACTIVITY:
                validate_no_trading_exclusion(exclusion, run_date)
            elif exclusion.get("reason") == TARGET_DATE_HISTORY_UNAVAILABLE:
                validate_target_date_unavailable_exclusion(exclusion, run_date)
            else:
                raise RuntimeError(
                    "Input preparation contains an unsupported exclusion reason."
                )
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
    preparation_exclusion_symbols: set[str],
    *,
    requested_symbols_count: int,
    prepared_symbols_count: int,
) -> None:
    if trend_symbols & preparation_exclusion_symbols:
        raise RuntimeError(
            "A Trend preparation exclusion also has a target-date Trend row."
        )
    if trend_symbols | preparation_exclusion_symbols != contract_symbols:
        raise RuntimeError(
            "Exact target-date Trend, exclusion, and contract symbol sets differ."
        )
    if prepared_symbols_count != len(trend_symbols):
        raise RuntimeError("Prepared Strategy count does not match target Trend rows.")
    if requested_symbols_count != len(contract_symbols):
        raise RuntimeError("Requested Strategy count does not match contract universe.")


def verify_inputs(
    engine,
    run_date: date,
    user_id: str,
    history_days: int = 5,
) -> dict:
    user_id = str(user_id).strip().upper()
    if not user_id:
        raise ValueError("An authoritative Strategy user id is required.")
    with engine.connect() as connection:
        preparation = load_input_preparation_manifest(connection, run_date, user_id)

    adapter = TrendIdentifierDbAdapter(
        engine=engine,
        run_date=run_date,
        user_id=user_id,
        history_days=history_days,
        require_exact_contract_snapshot=True,
    )
    trend_df, contract_df = adapter.load_source_frames()
    trend_symbols = set(trend_df["symbol"].astype(str)) if not trend_df.empty else set()
    contract_symbols = (
        set(contract_df["symbol"].astype(str)) if not contract_df.empty else set()
    )
    selected_trend_rows = list(
        trend_df[["symbol", "date"]].itertuples(index=False, name=None)
    )
    null_score_symbols = (
        set(
            trend_df.loc[trend_df["aggregate_score"].isna(), "symbol"].astype(str)
        )
        if not trend_df.empty
        else set()
    )
    contract_row_count = len(contract_df.index)

    if not trend_symbols:
        raise RuntimeError("Exact target-date Trend History has no symbols.")
    preparation_exclusion_symbols = {
        str(item["symbol"]).upper() for item in preparation["exclusions"]
    }
    validate_preparation_symbol_coverage(
        trend_symbols,
        contract_symbols,
        preparation_exclusion_symbols,
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

    strategy_inputs = adapter.build_all()
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
        "user_id": user_id,
        "trend_dates": trend_date_patterns[0]["dates"],
        "trend_date_patterns": trend_date_patterns,
        "trend_symbol_count": len(trend_symbols),
        "contract_symbol_count": len(contract_symbols),
        "requested_symbols_count": preparation["requested_symbols_count"],
        "prepared_symbols_count": preparation["prepared_symbols_count"],
        "strategy_input_count": len(strategy_inputs),
        "strategy_input_exclusion_count": len(all_exclusions),
        "strategy_input_exclusions": all_exclusions,
        "no_trading_activity_count": sum(
            item["reason"] == NO_TRADING_ACTIVITY
            for item in preparation["exclusions"]
        ),
        "no_trading_activity_symbols": sorted(
            str(item["symbol"]).upper()
            for item in preparation["exclusions"]
            if item["reason"] == NO_TRADING_ACTIVITY
        ),
        "target_date_history_unavailable_count": sum(
            item["reason"] == TARGET_DATE_HISTORY_UNAVAILABLE
            for item in preparation["exclusions"]
        ),
        "target_date_history_unavailable_symbols": sorted(
            str(item["symbol"]).upper()
            for item in preparation["exclusions"]
            if item["reason"] == TARGET_DATE_HISTORY_UNAVAILABLE
        ),
        "contract_snapshot_exact": True,
        "future_trend_rows_selected": 0,
    }


def main() -> None:
    args = build_argument_parser().parse_args()
    run_date = date.fromisoformat(args.run_date)
    payload = verify_inputs(get_engine(), run_date, args.user_id, args.history_days)
    print(json.dumps(payload, sort_keys=True, separators=(",", ":")))


if __name__ == "__main__":
    main()
