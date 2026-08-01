from __future__ import annotations

import json
from pathlib import Path

import pandas as pd


ACCURACY_CSV = Path("data/strategy_validation/directional_accuracy_report.csv")
SIGNALS_CSV = Path("data/strategy_validation/fo_universe_validation.csv")
OUTPUT_JSON = Path("data/strategy_validation/validation_dashboard_payload.json")


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.strip().lower() for c in df.columns]
    return df


def first_existing(row, keys):
    for key in keys:
        if key in row and row[key] not in ("", None):
            return row[key]
    return ""


def main():
    accuracy = normalize_columns(pd.read_csv(ACCURACY_CSV))
    signals = normalize_columns(pd.read_csv(SIGNALS_CSV))

    if "strength_bucket" not in signals.columns:
        signals["strength_bucket"] = pd.cut(
            signals["strength"],
            bins=[-1, 19, 39, 59, 79, 100],
            labels=["00-19", "20-39", "40-59", "60-79", "80-100"],
        )

    signal_rows = []

    for row in signals.fillna("").to_dict(orient="records"):
        close = first_existing(row, ["close", "signal_close", "close_price"])
        close_5d = first_existing(row, ["close_5d", "future_close", "close_plus_5d"])

        signal_rows.append({
            "run_date": first_existing(row, ["run_date", "trade_date"]),
            "symbol": first_existing(row, ["symbol"]),
            "regime": first_existing(row, ["regime", "regime_bucket"]),
            "strategy_family": first_existing(row, ["strategy_family"]),
            "strength_bucket": first_existing(row, ["strength_bucket"]),
            "strength": first_existing(row, ["strength", "final_strategy_strength"]),
            "confidence": first_existing(row, ["confidence", "conf"]),
            "score": first_existing(row, ["score"]),
            "close": close,
            "close_5d": close_5d,
            "return_5d_pct": first_existing(row, ["return_5d_pct"]),
        })

    payload = {
        "accuracy_rows": accuracy.fillna("").to_dict(orient="records"),
        "signal_rows": signal_rows,
    }

    OUTPUT_JSON.parent.mkdir(parents=True, exist_ok=True)

    OUTPUT_JSON.write_text(
        json.dumps(payload, indent=2, default=str),
        encoding="utf-8",
    )

    print("Saved:", OUTPUT_JSON)
    print("Accuracy rows:", len(payload["accuracy_rows"]))
    print("Signal rows:", len(payload["signal_rows"]))


if __name__ == "__main__":
    main()
