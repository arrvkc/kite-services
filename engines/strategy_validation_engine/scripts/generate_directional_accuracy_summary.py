from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate directional accuracy summary."
    )

    parser.add_argument(
        "--input",
        default="data/strategy_validation/fo_universe_validation.csv",
    )

    parser.add_argument(
        "--output",
        default="data/strategy_validation/directional_accuracy_summary.csv",
    )

    return parser.parse_args()


def compute_win_pct(df, threshold):
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

    df["win_0"] = compute_win_pct(df, 0)
    df["win_1"] = compute_win_pct(df, 1)
    df["win_2"] = compute_win_pct(df, 2)
    df["win_3"] = compute_win_pct(df, 3)

    summary = (
        df.groupby(
            [
                "regime",
                "strategy_family",
            ]
        )
        .agg(
            signals=("symbol", "count"),
            win_pct_0=("win_0", "mean"),
            win_pct_1=("win_1", "mean"),
            win_pct_2=("win_2", "mean"),
            win_pct_3=("win_3", "mean"),
        )
        .reset_index()
    )

    for col in [
        "win_pct_0",
        "win_pct_1",
        "win_pct_2",
        "win_pct_3",
    ]:
        summary[col] *= 100

    summary = summary.sort_values(
        [
            "regime",
            "strategy_family",
        ]
    )

    output_file = Path(args.output)

    output_file.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    summary.to_csv(
        output_file,
        index=False,
    )

    print()
    print("=" * 80)
    print("Saved:", output_file)
    print("Rows :", len(summary))
    print("=" * 80)
    print()
    print(summary.round(2))

if __name__ == "__main__":
    main()
