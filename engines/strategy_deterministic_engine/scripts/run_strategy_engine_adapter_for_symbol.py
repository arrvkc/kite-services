from __future__ import annotations

import argparse
from typing import Iterable

from kiteconnect import KiteConnect

from services.kite_credentials_service import get_kite_credentials
from engines.strategy_deterministic_engine.adapters.trend_identifier_adapter import TrendIdentifierKiteAdapter
from engines.strategy_deterministic_engine.family_selection import classify_regime, select_candidate_family
from engines.strategy_deterministic_engine.metrics import compute_history_metrics


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build Strategy Deterministic Engine adapter summary for one F&O stock symbol."
    )
    parser.add_argument("user_id")
    parser.add_argument("symbol")
    return parser


def _format_row(values: Iterable[object], widths: list[int]) -> str:
    return " | ".join(str(v).ljust(w) for v, w in zip(values, widths))


def summarize_strategy_input(strategy_input) -> list[str]:
    latest = strategy_input.latest_payload
    metrics = compute_history_metrics(strategy_input.trend_history_w5)

    regime_bucket = classify_regime(
        latest.label,
        latest.aggregate_score,
        metrics
    )

    candidate_family, _ = select_candidate_family(
        regime_bucket,
        latest.aggregate_score,
        latest.confidence,
        metrics
    )

    return [
        strategy_input.instrument,
        latest.label,
        f"{latest.confidence:.4f}",
        f"{latest.aggregate_score:.4f}",
        latest.internal_state,
        str(metrics.bull_count_5),
        str(metrics.bear_count_5),
        str(metrics.flat_count_5),
        str(metrics.sign_flip_count_5),
        f"{metrics.mean_score_3:.2f}",
        str(strategy_input.dte_near_month),
        "YES" if strategy_input.next_month_available else "NO",
        str(strategy_input.dte_next_month) if strategy_input.dte_next_month is not None else "-",
        regime_bucket,
        candidate_family,
    ]


def main() -> None:
    parser = build_argument_parser()
    args = parser.parse_args()

    api_key, access_token = get_kite_credentials(args.user_id)
    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)

    adapter = TrendIdentifierKiteAdapter(kite=kite)
    strategy_input = adapter.build_strategy_input_for_symbol(args.symbol.upper())

    columns = [
        ("SYMBOL", 12),
        ("LABEL", 8),
        ("CONF", 8),
        ("SCORE", 10),
        ("STATE", 14),
        ("BULL5", 7),
        ("BEAR5", 7),
        ("FLAT5", 7),
        ("SIGNFLIP5", 10),
        ("MEAN3", 8),
        ("NEAR_DTE", 10),
        ("NEXT_AVAIL", 12),
        ("NEXT_DTE", 10),
        ("REGIME", 10),
        ("CANDIDATE_FAMILY", 20),
    ]

    widths = [w for _, w in columns]
    row = summarize_strategy_input(strategy_input)

    print(" | ".join(name.ljust(w) for name, w in columns))
    print("-+-".join("-" * w for _, w in columns))
    print(_format_row(row, widths))


if __name__ == "__main__":
    main()
