from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate strength bucket heatmap report."
    )

    parser.add_argument(
        "--input",
        default="data/strategy_validation/directional_accuracy_report.csv",
    )

    parser.add_argument(
        "--output",
        default="data/strategy_validation/strength_bucket_heatmap_report.csv",
    )

    parser.add_argument(
        "--threshold",
        type=int,
        default=2,
        choices=[0, 1, 2, 3],
    )

    return parser.parse_args()


def main():
    args = parse_args()

    df = pd.read_csv(args.input)

    metric = f"win_pct_{args.threshold}"

    out = (
        df[df["scope"] == "ALL"]
        .pivot_table(
            index=[
                "regime",
                "strategy_family",
            ],
            columns="strength_bucket",
            values=metric,
            aggfunc="mean",
        )
        .reset_index()
    )

    output_file = Path(args.output)

    output_file.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    out.to_csv(
        output_file,
        index=False,
    )

    print()
    print("=" * 80)
    print("Saved:", output_file)
    print("Threshold:", args.threshold)
    print("=" * 80)
    print()
    print(
        out.round(2).to_string(index=False)
    )


if __name__ == "__main__":
    main()
