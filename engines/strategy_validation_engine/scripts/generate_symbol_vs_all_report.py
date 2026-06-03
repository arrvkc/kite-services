from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate ALL vs SYMBOL comparison report."
    )

    parser.add_argument(
        "--input",
        default="data/strategy_validation/directional_accuracy_report.csv",
    )

    parser.add_argument(
        "--symbol",
        required=True,
    )

    parser.add_argument(
        "--output",
        default=None,
    )

    return parser.parse_args()


def main():
    args = parse_args()

    symbol = args.symbol.upper()

    if args.output:
        output_file = Path(args.output)
    else:
        output_file = Path(
            f"data/strategy_validation/{symbol.lower()}_vs_all_report.csv"
        )

    df = pd.read_csv(args.input)

    all_df = (
        df[df["scope"] == "ALL"]
        .copy()
        .rename(
            columns={
                "signals": "all_signals",
                "win_pct_0": "all_win_pct_0",
                "win_pct_1": "all_win_pct_1",
                "win_pct_2": "all_win_pct_2",
                "win_pct_3": "all_win_pct_3",
            }
        )
    )

    sym_df = (
        df[
            (df["scope"] == "SYMBOL")
            & (df["symbol"] == symbol)
        ]
        .copy()
        .rename(
            columns={
                "signals": "symbol_signals",
                "win_pct_0": "symbol_win_pct_0",
                "win_pct_1": "symbol_win_pct_1",
                "win_pct_2": "symbol_win_pct_2",
                "win_pct_3": "symbol_win_pct_3",
            }
        )
    )

    merged = all_df.merge(
        sym_df,
        on=[
            "regime",
            "strategy_family",
            "strength_bucket",
        ],
        how="inner",
    )

    merged["lift_0"] = (
        merged["symbol_win_pct_0"]
        - merged["all_win_pct_0"]
    )

    merged["lift_1"] = (
        merged["symbol_win_pct_1"]
        - merged["all_win_pct_1"]
    )

    merged["lift_2"] = (
        merged["symbol_win_pct_2"]
        - merged["all_win_pct_2"]
    )

    merged["lift_3"] = (
        merged["symbol_win_pct_3"]
        - merged["all_win_pct_3"]
    )

    keep_cols = [
        "regime",
        "strategy_family",
        "strength_bucket",

        "all_signals",
        "all_win_pct_0",
        "all_win_pct_1",
        "all_win_pct_2",
        "all_win_pct_3",

        "symbol_signals",
        "symbol_win_pct_0",
        "symbol_win_pct_1",
        "symbol_win_pct_2",
        "symbol_win_pct_3",

        "lift_0",
        "lift_1",
        "lift_2",
        "lift_3",
    ]

    merged = merged[keep_cols]

    output_file.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    merged.to_csv(
        output_file,
        index=False,
    )

    print()
    print("=" * 80)
    print("Saved:", output_file)
    print("Rows :", len(merged))
    print("=" * 80)
    print()
    print(
        merged.round(2).to_string(index=False)
    )


if __name__ == "__main__":
    main()
