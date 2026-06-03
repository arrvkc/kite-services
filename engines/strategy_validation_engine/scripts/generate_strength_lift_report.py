from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


LOW_BUCKETS = ["00-19", "20-39"]
HIGH_BUCKETS = ["60-79", "80-100"]


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate strength lift report."
    )

    parser.add_argument(
        "--input",
        default="data/strategy_validation/fo_universe_validation.csv",
    )

    parser.add_argument(
        "--output",
        default="data/strategy_validation/strength_lift_report.csv",
    )

    return parser.parse_args()


def compute_success(df, threshold):
    result = pd.Series(False, index=df.index)

    bullish = df["strategy_family"].isin(
        ["BULL_CALL_SPREAD", "BULL_PUT_SPREAD"]
    )

    bearish = df["strategy_family"].isin(
        ["BEAR_CALL_SPREAD", "BEAR_PUT_SPREAD"]
    )

    iron = df["strategy_family"] == "IRON_CONDOR"

    result.loc[bullish] = (
        df.loc[bullish, "return_5d_pct"] > threshold
    )

    result.loc[bearish] = (
        df.loc[bearish, "return_5d_pct"] < -threshold
    )

    result.loc[iron] = (
        df.loc[iron, "return_5d_pct"].abs() <= 3.0
    )

    return result


def main():
    args = parse_args()

    df = pd.read_csv(args.input)

    df = df.dropna(subset=["return_5d_pct"]).copy()

    df["strength_bucket"] = pd.cut(
        df["strength"],
        bins=[-1, 19, 39, 59, 79, 100],
        labels=[
            "00-19",
            "20-39",
            "40-59",
            "60-79",
            "80-100",
        ],
    )

    rows = []

    for threshold in [0, 1, 2, 3]:

        df["success"] = compute_success(
            df,
            threshold,
        )

        for (regime, strategy), grp in df.groupby(
            ["regime", "strategy_family"]
        ):

            low = grp[
                grp["strength_bucket"].isin(
                    LOW_BUCKETS
                )
            ]

            high = grp[
                grp["strength_bucket"].isin(
                    HIGH_BUCKETS
                )
            ]

            low_signals = len(low)
            high_signals = len(high)

            low_win_pct = (
                low["success"].mean() * 100
                if low_signals
                else None
            )

            high_win_pct = (
                high["success"].mean() * 100
                if high_signals
                else None
            )

            lift = (
                high_win_pct - low_win_pct
                if (
                    low_win_pct is not None
                    and high_win_pct is not None
                )
                else None
            )

            rows.append(
                {
                    "regime": regime,
                    "strategy_family": strategy,
                    "threshold_pct": threshold,
                    "low_strength_signals": low_signals,
                    "high_strength_signals": high_signals,
                    "low_strength_win_pct": low_win_pct,
                    "high_strength_win_pct": high_win_pct,
                    "lift_pct": lift,
                }
            )

    out = pd.DataFrame(rows)

    out = out.sort_values(
        [
            "regime",
            "strategy_family",
            "threshold_pct",
        ]
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
        out.round(2).to_string(index=False)
    )


if __name__ == "__main__":
    main()
