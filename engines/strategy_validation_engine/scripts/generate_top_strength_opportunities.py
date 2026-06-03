from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate top strength opportunity report."
    )

    parser.add_argument(
        "--input",
        default="data/strategy_validation/fo_universe_validation.csv",
    )

    parser.add_argument(
        "--output",
        default="data/strategy_validation/top_strength_opportunities.csv",
    )

    parser.add_argument(
        "--min-strength",
        type=int,
        default=60,
    )

    parser.add_argument(
        "--top",
        type=int,
        default=100,
    )

    return parser.parse_args()


def main():
    args = parse_args()

    df = pd.read_csv(args.input)

    df = df.dropna(subset=["return_5d_pct"]).copy()

    df = df[df["strength"] >= args.min_strength]

    df["abs_return_5d_pct"] = (
        df["return_5d_pct"].abs()
    )

    cols = [
        "run_date",
        "symbol",
        "regime",
        "strategy_family",
        "strength",
        "confidence",
        "score",
        "transition_state",
        "return_5d_pct",
        "abs_return_5d_pct",
    ]

    out = (
        df[cols]
        .sort_values(
            [
                "strength",
                "abs_return_5d_pct",
            ],
            ascending=False,
        )
        .head(args.top)
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
    print("Rows :", len(out))
    print("=" * 80)
    print()
    print(
        out.head(50).round(2).to_string(index=False)
    )


if __name__ == "__main__":
    main()
