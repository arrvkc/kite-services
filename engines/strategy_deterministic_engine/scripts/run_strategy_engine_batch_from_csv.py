from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Dict, Iterable, List, Sequence

from engines.strategy_deterministic_engine.adapters.trend_identifier_batch_adapter import TrendIdentifierBatchAdapter
from engines.strategy_deterministic_engine.engine import evaluate_batch
from engines.strategy_deterministic_engine.family_selection import classify_regime, select_candidate_family
from engines.strategy_deterministic_engine.metrics import compute_history_metrics


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run Strategy Deterministic Engine for a precomputed F&O universe trend-history CSV and contract snapshot CSV."
    )
    parser.add_argument(
        "--trend-history-csv",
        default="data/trend_history_fo_universe.csv",
        help="Combined trend history CSV path",
    )
    parser.add_argument(
        "--contract-snapshot-csv",
        default="data/contract_snapshot_fo_universe.csv",
        help="Contract snapshot CSV path",
    )
    parser.add_argument(
        "--output-csv",
        default=None,
        help="Optional path to write the batch output as CSV",
    )
    return parser


def _build_input_map(strategy_inputs: Iterable[object]) -> Dict[str, object]:
    result: Dict[str, object] = {}
    for item in strategy_inputs:
        result[str(item.instrument)] = item
    return result


def _stringify_reason_codes(payload: object) -> str:
    reason_codes = getattr(payload, "reason_codes", None)
    if reason_codes is None and isinstance(payload, dict):
        reason_codes = payload.get("reason_codes")
    if not reason_codes:
        return "-"
    return ",".join(str(item) for item in reason_codes)


def output_to_row(payload: object, strategy_input: object) -> List[str]:
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

    def getv(obj, key):
        if isinstance(obj, dict):
            return obj.get(key)
        return getattr(obj, key)

    return [
        getv(payload, "instrument"),
        latest.label,
        f"{latest.aggregate_score:.2f}" if latest.aggregate_score is not None else "null",
        f"{latest.confidence:.4f}",
        latest.internal_state,
        str(metrics.bull_count_5),
        str(metrics.bear_count_5),
        str(metrics.flat_count_5),
        str(metrics.sign_flip_count_5),
        f"{metrics.mean_score_3:.2f}",
        str(strategy_input.dte_near_month),
        str(strategy_input.dte_next_month) if strategy_input.dte_next_month is not None else "-",
        regime_bucket,
        candidate_family,
        getv(payload, "strategy_family"),
        getv(payload, "contract_month_selection"),
        str(getv(payload, "final_strategy_strength")),
        "YES" if getv(payload, "include_in_top_n") else "NO",
        str(getv(payload, "rank_overall")) if getv(payload, "rank_overall") is not None else "",
        str(getv(payload, "rank_in_family")) if getv(payload, "rank_in_family") is not None else "",
        getv(payload, "strategy_transition_state"),
        _stringify_reason_codes(payload),
    ]


def _format_cell(value: object, width: int) -> str:
    text = str(value)
    if len(text) > width:
        if width <= 1:
            return text[:width]
        return text[: width - 1] + "…"
    return text.ljust(width)


def _format_row(values: Sequence[object], columns: Sequence[tuple[str, int]]) -> str:
    return " | ".join(_format_cell(value, width) for value, (_, width) in zip(values, columns))


def main() -> None:
    parser = build_argument_parser()
    args = parser.parse_args()

    adapter = TrendIdentifierBatchAdapter.from_csv(
        trend_history_csv_path=args.trend_history_csv,
        contract_snapshot_csv_path=args.contract_snapshot_csv,
    )
    strategy_inputs = adapter.build_all()
    batch_result = evaluate_batch(strategy_inputs)

    results = batch_result["results"]
    ranked_outputs = [item["payload"] for item in results if item["mode"] == "public_payload"]
    invalid_evaluations = [item for item in results if item["mode"] != "public_payload"]

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

    try:
        for payload in ranked_outputs:
            strategy_input = input_map.get(str(payload["instrument"]))
            if strategy_input is None:
                continue
            row = output_to_row(payload, strategy_input)
            print(_format_row(row, columns))
            if csv_writer is not None:
                csv_writer.writerow(row)
    finally:
        if csv_file is not None:
            csv_file.close()

    if invalid_evaluations:
        print("")
        print(f"Invalid evaluations excluded from ranking: {len(invalid_evaluations)}")


if __name__ == "__main__":
    main()
