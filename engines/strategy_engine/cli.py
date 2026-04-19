"""CLI for the Strategy Engine."""
from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
from typing import Any, List

from .adapters.trend_identifier_adapter import strategy_input_from_dict
from .engine import evaluate_batch, evaluate_strategy_engine


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run Strategy Engine v1.2")
    parser.add_argument("--input-file", required=True, help="Path to JSON input file")
    parser.add_argument("--output-file", help="Optional path to write JSON output")
    parser.add_argument("--batch", action="store_true", help="Treat input as a list of instruments")
    parser.add_argument("--log-level", default="INFO", help="Logging level")
    return parser


def _load_json(path: str) -> Any:
    return json.loads(Path(path).read_text())


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO))
    data = _load_json(args.input_file)

    if args.batch:
        inputs = [strategy_input_from_dict(item) for item in data]
        payloads, _ = evaluate_batch(inputs)
        result: List[dict[str, Any]] = [payload.to_dict() for payload in payloads]
    else:
        strategy_input = strategy_input_from_dict(data)
        payload, _ = evaluate_strategy_engine(strategy_input)
        result = payload.to_dict()

    rendered = json.dumps(result, indent=2)
    if args.output_file:
        Path(args.output_file).write_text(rendered)
    print(rendered)


if __name__ == "__main__":
    main()
