from __future__ import annotations

import argparse
import json
from dataclasses import asdict, is_dataclass
from typing import Any

from kiteconnect import KiteConnect

from services.kite_credentials_service import get_kite_credentials
from engines.strategy_engine.adapters.trend_identifier_adapter import TrendIdentifierKiteAdapter


def _to_jsonable(value: Any) -> Any:
    if is_dataclass(value):
        return {k: _to_jsonable(v) for k, v in asdict(value).items()}
    if isinstance(value, list):
        return [_to_jsonable(item) for item in value]
    if isinstance(value, tuple):
        return [_to_jsonable(item) for item in value]
    if isinstance(value, dict):
        return {str(k): _to_jsonable(v) for k, v in value.items()}
    return value


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build Strategy Engine input for one F&O stock symbol using KiteConnect-backed Trend Identifier adapter."
    )
    parser.add_argument("user_id", help="Zerodha user id used to fetch Kite credentials")
    parser.add_argument("symbol", help="F&O stock symbol, for example ABB")
    return parser

def summarize_strategy_input(si):
    latest = si.latest_payload
    history = si.trend_history_w5

    bull = sum(1 for r in history if r.label == "UP" or (r.label == "FLAT" and r.aggregate_score >= 10))
    bear = sum(1 for r in history if r.label == "DOWN" or (r.label == "FLAT" and r.aggregate_score <= -10))
    flat = sum(1 for r in history if r.label == "FLAT" and abs(r.aggregate_score) <= 10)

    likely_family = "NO_TRADE"
    if (
        latest.label == "UP"
        and latest.aggregate_score >= 40
        and latest.confidence >= 0.60
        and bull >= 3
    ):
        likely_family = "BULL_CALL_SPREAD"
    elif (
        latest.label == "DOWN"
        and latest.aggregate_score <= -40
        and latest.confidence >= 0.60
        and bear >= 3
    ):
        likely_family = "BEAR_PUT_SPREAD"

    return [
        si.instrument,
        latest.label,
        f"{latest.confidence:.4f}",
        f"{latest.aggregate_score:.4f}",
        str(bull),
        str(bear),
        str(flat),
        str(si.dte_near_month),
        "YES" if si.next_month_available else "NO",
        str(si.dte_next_month) if si.dte_next_month is not None else "-",
        likely_family,
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
        ("BULL5", 7),
        ("BEAR5", 7),
        ("FLAT5", 7),
        ("NEAR_DTE", 10),
        ("NEXT_AVAIL", 12),
        ("NEXT_DTE", 10),
        ("LIKELY_FAMILY", 20),
    ]

    row = summarize_strategy_input(strategy_input)

    header_line = " | ".join(name.ljust(width) for name, width in columns)
    separator_line = "-+-".join("-" * width for _, width in columns)
    row_line = " | ".join(str(value).ljust(width) for value, (_, width) in zip(row, columns))

    print(header_line)
    print(separator_line)
    print(row_line)


if __name__ == "__main__":
    main()