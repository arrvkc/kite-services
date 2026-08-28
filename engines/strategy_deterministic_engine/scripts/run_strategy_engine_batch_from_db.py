from __future__ import annotations

import argparse
import csv
from datetime import date
from pathlib import Path
from typing import Dict, Iterable, List, Sequence

from engines.strategy_deterministic_engine.adapters.trend_identifier_db_adapter import (
    TrendIdentifierDbAdapter,
)
from engines.strategy_deterministic_engine.db.postgres import get_engine
from engines.strategy_deterministic_engine.db.upserts import (
    clear_strategy_batch_results_for_run_date,
    complete_strategy_run,
    create_or_restart_strategy_run,
    upsert_strategy_batch_result_rows,
)
from engines.strategy_deterministic_engine.engine import evaluate_batch
from engines.strategy_deterministic_engine.family_selection import (
    classify_regime,
    select_candidate_family,
)
from engines.strategy_deterministic_engine.metrics import compute_history_metrics
from engines.strategy_deterministic_engine.scripts.verify_strategy_backfill_inputs import (
    verify_inputs,
)


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run Strategy Deterministic Engine from ATMS DB and store batch results."
    )
    parser.add_argument("user_id", help="User id used only as generated_by_user_id metadata")
    parser.add_argument("--run-date", default=date.today().isoformat(), help="Run date YYYY-MM-DD")
    parser.add_argument("--history-days", type=int, default=5)
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--output-csv", default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--require-exact-contract-snapshot",
        action="store_true",
        help="Reject fallback contract snapshots before a historical recovery run.",
    )
    return parser


def _build_input_map(strategy_inputs: Iterable[object]) -> Dict[str, object]:
    return {str(item.instrument): item for item in strategy_inputs}


def _stringify_reason_codes(payload: object) -> str:
    reason_codes = getattr(payload, "reason_codes", None)
    if reason_codes is None and isinstance(payload, dict):
        reason_codes = payload.get("reason_codes")
    if not reason_codes:
        return "-"
    return ",".join(str(item) for item in reason_codes)


def _getv(obj, key):
    if isinstance(obj, dict):
        return obj.get(key)
    return getattr(obj, key)


def output_to_row(payload: object, strategy_input: object) -> List[object]:
    latest = strategy_input.latest_payload
    metrics = compute_history_metrics(strategy_input.trend_history_w5)

    regime_bucket = classify_regime(
        latest.label,
        latest.aggregate_score,
        metrics,
    )

    candidate_family, _ = select_candidate_family(
        regime_bucket,
        latest.aggregate_score,
        latest.confidence,
        metrics,
    )

    return [
        _getv(payload, "instrument"),
        latest.label,
        latest.aggregate_score,
        latest.confidence,
        latest.internal_state,
        metrics.bull_count_5,
        metrics.bear_count_5,
        metrics.flat_count_5,
        metrics.sign_flip_count_5,
        metrics.mean_score_3,
        strategy_input.dte_near_month,
        strategy_input.dte_next_month,
        regime_bucket,
        candidate_family,
        _getv(payload, "strategy_family"),
        _getv(payload, "contract_month_selection"),
        _getv(payload, "final_strategy_strength"),
        bool(_getv(payload, "include_in_top_n")),
        _getv(payload, "rank_overall"),
        _getv(payload, "rank_in_family"),
        _getv(payload, "strategy_transition_state"),
        _stringify_reason_codes(payload),
    ]


def output_to_db_row(payload: object, strategy_input: object, run_id: int, run_date: date) -> dict:
    row = output_to_row(payload, strategy_input)

    return {
        "run_id": run_id,
        "run_date": run_date,
        "symbol": row[0],
        "label": row[1],
        "score": row[2],
        "confidence": row[3],
        "state": row[4],
        "bull_count_5": row[5],
        "bear_count_5": row[6],
        "flat_count_5": row[7],
        "sign_flip_count_5": row[8],
        "mean_score_3": row[9],
        "dte_near_month": row[10],
        "dte_next_month": row[11],
        "regime_bucket": row[12],
        "candidate_family": row[13],
        "strategy_family": row[14],
        "contract_month_selection": row[15],
        "final_strategy_strength": row[16],
        "include_in_top_n": row[17],
        "rank_overall": row[18],
        "rank_in_family": row[19],
        "strategy_transition_state": row[20],
        "reason_codes": row[21],
    }


def _format_cell(value: object, width: int) -> str:
    text = "" if value is None else str(value)
    if isinstance(value, float):
        text = f"{value:.4f}"
    if len(text) > width:
        return text[: width - 1] + "…" if width > 1 else text[:width]
    return text.ljust(width)


def _format_row(values: Sequence[object], columns: Sequence[tuple[str, int]]) -> str:
    return " | ".join(_format_cell(value, width) for value, (_, width) in zip(values, columns))


def main() -> None:
    args = build_argument_parser().parse_args()
    run_date = date.fromisoformat(args.run_date)

    engine = get_engine()

    input_verification = None
    if args.require_exact_contract_snapshot:
        input_verification = verify_inputs(engine, run_date, args.history_days)

    adapter = TrendIdentifierDbAdapter(
        engine=engine,
        run_date=run_date,
        history_days=args.history_days,
        require_exact_contract_snapshot=args.require_exact_contract_snapshot,
    )
    strategy_inputs = adapter.build_all()
    batch_result = evaluate_batch(strategy_inputs)

    results = batch_result["results"]
    ranked_outputs = [item["payload"] for item in results if item["mode"] == "public_payload"]
    invalid_evaluations = [item for item in results if item["mode"] != "public_payload"]

    if len(ranked_outputs) + len(invalid_evaluations) != len(strategy_inputs):
        raise RuntimeError("Strategy evaluation count did not match its verified inputs.")
    if (
        input_verification is not None
        and input_verification["strategy_input_count"] != len(strategy_inputs)
    ):
        raise RuntimeError("Strategy inputs changed after exact-date verification.")

    input_map = _build_input_map(strategy_inputs)

    columns = [
        ("SYMBOL", 12),
        ("LABEL", 8),
        ("SCORE", 10),
        ("CONF", 8),
        ("STATE", 14),
        ("BULL5", 7),
        ("BEAR5", 7),
        ("FLAT5", 7),
        ("SIGNFLIP5", 10),
        ("MEAN3", 8),
        ("NEAR_DTE", 10),
        ("NEXT_DTE", 10),
        ("REGIME", 10),
        ("CANDIDATE_FAMILY", 20),
        ("STRATEGY_FAMILY", 20),
        ("CONTRACT_MONTH", 18),
        ("STRENGTH", 10),
        ("TOP_N", 8),
        ("RANK_ALL", 10),
        ("RANK_FAMILY", 13),
        ("TRANSITION_STATE", 20),
        ("REASONS", 40),
    ]

    print(" | ".join(name.ljust(width) for name, width in columns))
    print("-+-".join("-" * width for _, width in columns))

    csv_file = None
    csv_writer = None
    if args.output_csv:
        output_path = Path(args.output_csv)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        csv_file = output_path.open("w", newline="", encoding="utf-8")
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow([name for name, _ in columns])

    db_rows = []

    try:
        for payload in ranked_outputs:
            strategy_input = input_map.get(str(payload["instrument"]))
            if strategy_input is None:
                continue

            display_row = output_to_row(payload, strategy_input)
            print(_format_row(display_row, columns))

            if csv_writer is not None:
                csv_writer.writerow(display_row)

            db_rows.append(
                output_to_db_row(
                    payload=payload,
                    strategy_input=strategy_input,
                    run_id=0,
                    run_date=run_date,
                )
            )
    finally:
        if csv_file is not None:
            csv_file.close()

    if args.dry_run:
        print("")
        print(f"DRY RUN: strategy_inputs={len(strategy_inputs)} public_results={len(ranked_outputs)} invalid={len(invalid_evaluations)} db_rows={len(db_rows)}")
        return

    input_provenance = None
    if input_verification is not None:
        input_provenance = {
            "requested_symbols_count": input_verification[
                "requested_symbols_count"
            ],
            "prepared_symbols_count": input_verification[
                "prepared_symbols_count"
            ],
            "evaluated_symbols_count": len(strategy_inputs),
            "input_exclusions": input_verification[
                "strategy_input_exclusions"
            ],
        }

    with engine.begin() as conn:
        run_id = create_or_restart_strategy_run(
            conn=conn,
            run_date=run_date,
            generated_by_user_id=args.user_id,
            input_provenance=input_provenance,
        )

        for row in db_rows:
            row["run_id"] = run_id

        cleared = clear_strategy_batch_results_for_run_date(conn, run_date)
        written = upsert_strategy_batch_result_rows(
            conn=conn,
            rows=db_rows,
            batch_size=args.batch_size,
        )

        complete_strategy_run(
            conn=conn,
            run_id=run_id,
            status="COMPLETED",
            total_symbols=len(strategy_inputs),
            public_results_count=len(ranked_outputs),
            invalid_count=len(invalid_evaluations),
        )

    print("")
    print(f"CLEARED stale strategy_deterministic_engine_batch_results rows={cleared}")
    print(f"UPSERTED strategy_deterministic_engine_batch_results rows={written}")
    print(f"COMPLETED strategy_deterministic_engine_runs run_id={run_id}")
    print(f"Invalid evaluations excluded from ranking: {len(invalid_evaluations)}")


if __name__ == "__main__":
    main()
