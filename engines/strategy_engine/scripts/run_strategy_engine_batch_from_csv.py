from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Dict, Iterable, List, Sequence

from engines.strategy_engine.adapters.trend_identifier_batch_adapter import TrendIdentifierBatchAdapter
from engines.strategy_engine.engine import evaluate_batch


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run Strategy Engine for a precomputed F&O universe trend-history CSV and contract snapshot CSV."
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


def _score_sign(score: float) -> int:
    if abs(score) < 10:
        return 0
    if score > 0:
        return 1
    if score < 0:
        return -1
    return 0


def _count_sign_flips(scores: Sequence[float]) -> int:
    flips = 0
    if len(scores) < 2:
        return 0
    mapped = [_score_sign(score) for score in scores]
    for prev, curr in zip(mapped, mapped[1:]):
        if prev != curr:
            flips += 1
    return flips


def _compute_history_metrics(strategy_input) -> Dict[str, float | int]:
    history = strategy_input.trend_history_w5
    scores = [float(row.aggregate_score) for row in history]
    bull_count_5 = sum(
        1
        for row in history
        if row.label == "UP" or (row.label == "FLAT" and float(row.aggregate_score) >= 10)
    )
    bear_count_5 = sum(
        1
        for row in history
        if row.label == "DOWN" or (row.label == "FLAT" and float(row.aggregate_score) <= -10)
    )
    flat_count_5 = sum(
        1
        for row in history
        if row.label == "FLAT" and abs(float(row.aggregate_score)) <= 10
    )
    mean_score_3 = sum(scores[-3:]) / 3
    sign_flip_count_5 = _count_sign_flips(scores)
    return {
        "bull_count_5": bull_count_5,
        "bear_count_5": bear_count_5,
        "flat_count_5": flat_count_5,
        "mean_score_3": mean_score_3,
        "sign_flip_count_5": sign_flip_count_5,
    }


def _select_candidate_family(strategy_input, metrics: Dict[str, float | int]) -> str:
    latest = strategy_input.latest_payload
    label = latest.label
    score = latest.aggregate_score
    confidence = latest.confidence

    if score is None:
        return "NO_TRADE"

    bull_count_5 = int(metrics["bull_count_5"])
    bear_count_5 = int(metrics["bear_count_5"])
    flat_count_5 = int(metrics["flat_count_5"])
    mean_score_3 = float(metrics["mean_score_3"])
    sign_flip_count_5 = int(metrics["sign_flip_count_5"])

    if (
        label == "UP"
        and score >= 40
        and confidence >= 0.60
        and bull_count_5 >= 3
    ):
        return "BULL_CALL_SPREAD"

    if (
        label == "DOWN"
        and score <= -40
        and confidence >= 0.60
        and bear_count_5 >= 3
    ):
        return "BEAR_PUT_SPREAD"

    if (
        label == "FLAT"
        and abs(score) <= 10
        and confidence >= 0.50
        and flat_count_5 >= 4
        and abs(mean_score_3) <= 8
        and sign_flip_count_5 <= 1
    ):
        return "IRON_CONDOR"

    if (
        (
            (label == "UP" and 10 < score < 40)
            or (label == "FLAT" and 10 <= score < 25)
        )
        and confidence >= 0.45
        and bull_count_5 >= 3
    ):
        return "BULL_PUT_SPREAD"

    if (
        (
            (label == "DOWN" and -40 < score < -10)
            or (label == "FLAT" and -25 < score <= -10)
        )
        and confidence >= 0.45
        and bear_count_5 >= 3
    ):
        return "BEAR_CALL_SPREAD"

    return "NO_TRADE"


def _stringify_reason_codes(payload) -> str:
    reason_codes = getattr(payload, "reason_codes", None)
    if not reason_codes:
        return "-"
    return ",".join(str(item) for item in reason_codes)


def _build_input_map(strategy_inputs: Iterable[object]) -> Dict[str, object]:
    result: Dict[str, object] = {}
    for item in strategy_inputs:
        result[str(item.instrument)] = item
    return result


def output_to_row(payload, strategy_input) -> List[str]:
    latest = strategy_input.latest_payload
    metrics = _compute_history_metrics(strategy_input)
    candidate_family = _select_candidate_family(strategy_input, metrics)

    return [
        payload.instrument,
        latest.label,
        f"{latest.aggregate_score:.2f}" if latest.aggregate_score is not None else "null",
        f"{latest.confidence:.4f}",
        latest.internal_state,
        str(metrics["bull_count_5"]),
        str(metrics["bear_count_5"]),
        str(metrics["flat_count_5"]),
        str(metrics["sign_flip_count_5"]),
        f"{float(metrics['mean_score_3']):.2f}",
        str(strategy_input.dte_near_month),
        str(strategy_input.dte_next_month) if strategy_input.dte_next_month is not None else "-",
        candidate_family,
        payload.strategy_family,
        payload.contract_month_selection,
        str(payload.final_strategy_strength),
        "YES" if payload.include_in_top_n else "NO",
        str(payload.rank_overall) if payload.rank_overall is not None else "-",
        str(payload.rank_in_family) if payload.rank_in_family is not None else "-",
        payload.strategy_transition_state,
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
    ranked_outputs, _ = evaluate_batch(strategy_inputs)

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
        ("CANDIDATE_FAMILY", 20),
        ("STRATEGY_FAMILY", 20),
        ("CONTRACT_MONTH", 18),
        ("STRENGTH", 10),
        ("TOP_N", 8),
        ("RANK_ALL", 10),
        ("RANK_FAMILY", 13),
        ("TRANSITION_STATE", 20),
        ("REASONS", 36),
    ]

    header_line = " | ".join(name.ljust(width) for name, width in columns)
    separator_line = "-+-".join("-" * width for _, width in columns)

    print(header_line)
    print(separator_line)

    csv_headers = [name for name, _ in columns]
    csv_file = None
    csv_writer = None

    if args.output_csv:
        output_path = Path(args.output_csv)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        csv_file = output_path.open("w", newline="", encoding="utf-8")
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(csv_headers)

    try:
        for payload in ranked_outputs:
            strategy_input = input_map.get(str(payload.instrument))
            if strategy_input is None:
                continue
            row = output_to_row(payload, strategy_input)
            print(_format_row(row, columns))
            if csv_writer is not None:
                csv_writer.writerow(row)
    finally:
        if csv_file is not None:
            csv_file.close()


if __name__ == "__main__":
    main()
