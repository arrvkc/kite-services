from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate research dashboard JSON."
    )

    parser.add_argument(
        "--validation",
        default="data/strategy_validation/fo_universe_validation.csv",
    )

    parser.add_argument(
        "--accuracy",
        default="data/strategy_validation/directional_accuracy_report.csv",
    )

    parser.add_argument(
        "--summary",
        default="data/strategy_validation/directional_accuracy_summary.csv",
    )

    parser.add_argument(
        "--output",
        default="data/strategy_validation/research_dashboard.json",
    )

    return parser.parse_args()


def main():
    args = parse_args()

    validation = pd.read_csv(args.validation)
    accuracy = pd.read_csv(args.accuracy)
    summary = pd.read_csv(args.summary)

    validation = validation.dropna(
        subset=["return_5d_pct"]
    )

    dashboard = {}

    dashboard["summary"] = {
        "total_signals": int(len(validation)),
        "total_symbols": int(
            validation["symbol"].nunique()
        ),
        "date_range": {
            "from": str(
                validation["run_date"].min()
            ),
            "to": str(
                validation["run_date"].max()
            ),
        },
    }

    dashboard["strategy_summary"] = (
        summary.fillna("")
        .to_dict(
            orient="records"
        )
    )

    dashboard["heatmap"] = (
        accuracy[
            accuracy["scope"] == "ALL"
        ]
        .fillna("")
        .to_dict(
            orient="records"
        )
    )

    symbol_vs_all = {}

    symbol_rows = accuracy[
        accuracy["scope"] == "SYMBOL"
    ]

    for symbol, grp in symbol_rows.groupby(
        "symbol"
    ):
        symbol_vs_all[symbol] = (
            grp.fillna("")
            .to_dict(
                orient="records"
            )
        )

    dashboard["symbols"] = symbol_vs_all

    dashboard["signal_details"] = (
        validation[
            [
                "run_date",
                "symbol",
                "regime",
                "strategy_family",
                "strength",
                "confidence",
                "score",
                "transition_state",
                "return_5d_pct",
            ]
        ]
        .fillna("")
        .to_dict(
            orient="records"
        )
    )

    output_file = Path(args.output)

    output_file.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with open(
        output_file,
        "w",
        encoding="utf-8",
    ) as f:
        json.dump(
            dashboard,
            f,
            indent=2,
            default=str,
        )

    print()
    print("=" * 80)
    print("Saved:", output_file)
    print("=" * 80)

    print()
    print(
        "Summary:",
        dashboard["summary"]
    )

    print(
        "Strategies:",
        len(
            dashboard["strategy_summary"]
        )
    )

    print(
        "Heatmap Rows:",
        len(
            dashboard["heatmap"]
        )
    )

    print(
        "Symbols:",
        len(
            dashboard["symbols"]
        )
    )

    print(
        "Signal Details:",
        len(
            dashboard["signal_details"]
        )
    )


if __name__ == "__main__":
    main()
