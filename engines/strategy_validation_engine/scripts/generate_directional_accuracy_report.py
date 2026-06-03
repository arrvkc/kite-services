from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate directional accuracy report."
    )

    parser.add_argument(
        "--input",
        default="data/strategy_validation/fo_universe_validation.csv",
    )

    parser.add_argument(
        "--output",
        default="data/strategy_validation/directional_accuracy_report.csv",
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


def build_scope(df, scope_name, symbol=None):
    work = df.copy()

    if symbol:
        work = work[work["symbol"] == symbol]

    if len(work) == 0:
        return pd.DataFrame()

    work["win_0"] = compute_win_pct(work, 0)
    work["win_1"] = compute_win_pct(work, 1)
    work["win_2"] = compute_win_pct(work, 2)
    work["win_3"] = compute_win_pct(work, 3)

    summary = (
        work.groupby(
            [
                "regime",
                "strategy_family",
                "strength_bucket",
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

    summary["win_pct_0"] *= 100
    summary["win_pct_1"] *= 100
    summary["win_pct_2"] *= 100
    summary["win_pct_3"] *= 100

    summary.insert(0, "scope", scope_name)
    summary.insert(1, "symbol", symbol or "ALL")

    return summary


def main():
    args = parse_args()

    input_file = Path(args.input)
    output_file = Path(args.output)

    df = pd.read_csv(input_file)

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

    reports = []

    reports.append(
        build_scope(df, "ALL")
    )

    for symbol in sorted(df["symbol"].unique()):
        reports.append(
            build_scope(
                df,
                "SYMBOL",
                symbol,
            )
        )

    final_df = pd.concat(
        reports,
        ignore_index=True,
    )

    final_df = final_df.sort_values(
        [
            "scope",
            "symbol",
            "regime",
            "strategy_family",
            "strength_bucket",
        ]
    )

    output_file.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    final_df.to_csv(
        output_file,
        index=False,
    )

    print()
    print("=" * 80)
    print("Saved:", output_file)
    print("Rows :", len(final_df))
    print("=" * 80)

    print()
    print(final_df.head(20).round(2))


if __name__ == "__main__":
    main()
