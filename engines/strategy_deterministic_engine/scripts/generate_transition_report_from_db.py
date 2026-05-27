from __future__ import annotations

import subprocess
from io import StringIO
from pathlib import Path

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[3]
OUTPUT_DIR = REPO_ROOT / "data" / "strategy_transition_reports"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

TRANSITION_CSV = OUTPUT_DIR / "strategy_transition_scanner_latest.csv"
LATEST_HTML = OUTPUT_DIR / "strategy_transition_report_latest.html"

SQL = """
SELECT
    run_date,
    symbol,
    label,
    confidence,
    regime_bucket,
    strategy_family,
    final_strategy_strength,
    include_in_top_n,
    rank_overall,
    rank_in_family,
    strategy_transition_state,
    reason_codes
FROM strategy_deterministic_engine_batch_results
ORDER BY symbol, run_date
"""


def query_df(sql: str) -> pd.DataFrame:
    cmd = [
        "docker", "exec", "-i", "postgres",
        "psql", "-U", "postgres", "-d", "atms",
        "-c", f"COPY ({sql}) TO STDOUT WITH CSV HEADER",
    ]
    out = subprocess.check_output(cmd, text=True)
    return pd.read_csv(StringIO(out))


def enrich_price_only(df: pd.DataFrame) -> pd.DataFrame:
    trend_sql = """
    SELECT
        symbol,
        trade_date,
        close
    FROM trend_history_fo_universe
    ORDER BY symbol, trade_date
    """

    trend = query_df(trend_sql)

    trend["trade_date"] = pd.to_datetime(trend["trade_date"])
    trend = trend.sort_values(["symbol", "trade_date"])

    trend["previous_close"] = trend.groupby("symbol")["close"].shift(1)
    trend["price_change_pct"] = (
        (trend["close"] - trend["previous_close"])
        / trend["previous_close"]
    ) * 100

    trend["volume_ratio_20d"] = None
    trend = trend.rename(columns={"trade_date": "run_date"})

    return df.merge(
        trend[["symbol", "run_date", "close", "price_change_pct", "volume_ratio_20d"]],
        on=["symbol", "run_date"],
        how="left",
    )


def build_transition_rows(df: pd.DataFrame) -> pd.DataFrame:
    rows = []

    df["run_date"] = pd.to_datetime(df["run_date"])
    df["final_strategy_strength"] = pd.to_numeric(df["final_strategy_strength"], errors="coerce")
    df["confidence"] = pd.to_numeric(df["confidence"], errors="coerce")
    df["include_in_top_n"] = df["include_in_top_n"].astype(str).str.lower().isin(["t", "true", "1", "yes"])

    df = df.sort_values(["symbol", "run_date"])

    for symbol, g in df.groupby("symbol"):
        g = g.sort_values("run_date").reset_index(drop=True)

        for i in range(1, len(g)):
            prev = g.iloc[i - 1]
            curr = g.iloc[i]

            change_types = []

            if prev["strategy_family"] != curr["strategy_family"]:
                change_types.append("STRATEGY_FAMILY_CHANGE")

            if prev["regime_bucket"] != curr["regime_bucket"]:
                change_types.append("REGIME_BUCKET_CHANGE")

            if bool(prev["include_in_top_n"]) != bool(curr["include_in_top_n"]):
                change_types.append("TOPN_ENTERED" if bool(curr["include_in_top_n"]) else "TOPN_EXITED")

            strength_delta = curr["final_strategy_strength"] - prev["final_strategy_strength"]
            confidence_delta = curr["confidence"] - prev["confidence"]

            if pd.notna(strength_delta) and abs(strength_delta) >= 15:
                change_types.append("STRENGTH_JUMP")

            if pd.notna(confidence_delta) and abs(confidence_delta) >= 0.05:
                change_types.append("CONFIDENCE_JUMP")

            if prev["strategy_transition_state"] != curr["strategy_transition_state"]:
                change_types.append("TRANSITION_STATE_CHANGE")

            if not change_types:
                continue

            rows.append(
                {
                    "run_date": curr["run_date"],
                    "symbol": symbol,
                    "change_types": ",".join(change_types),
                    "previous_regime_bucket": prev["regime_bucket"],
                    "current_regime_bucket": curr["regime_bucket"],
                    "previous_strategy_family": prev["strategy_family"],
                    "current_strategy_family": curr["strategy_family"],
                    "previous_strength": prev["final_strategy_strength"],
                    "current_strength": curr["final_strategy_strength"],
                    "strength_delta": round(strength_delta, 2) if pd.notna(strength_delta) else None,
                    "previous_confidence": prev["confidence"],
                    "current_confidence": curr["confidence"],
                    "confidence_delta": round(confidence_delta, 4) if pd.notna(confidence_delta) else None,
                    "previous_transition_state": prev["strategy_transition_state"],
                    "current_transition_state": curr["strategy_transition_state"],
                    "previous_include_in_top_n": prev["include_in_top_n"],
                    "current_include_in_top_n": curr["include_in_top_n"],
                    "reason_codes": curr.get("reason_codes", ""),
                }
            )

    return pd.DataFrame(rows)


def main() -> None:
    source = query_df(SQL)

    transitions = build_transition_rows(source)

    if transitions.empty:
        transitions.to_csv(TRANSITION_CSV, index=False)
        print("No transitions found.")
        return

    latest_date = transitions["run_date"].max()
    transitions = transitions[transitions["run_date"] == latest_date].copy()

    transitions = enrich_price_only(transitions)
    transitions["run_date"] = pd.to_datetime(transitions["run_date"]).dt.date.astype(str)

    transitions.to_csv(TRANSITION_CSV, index=False)
    print(f"Saved transition CSV: {TRANSITION_CSV}")

    subprocess.check_call(
        [
            "python",
            str(REPO_ROOT / "engines" / "strategy_deterministic_engine" / "scripts" / "generate_transition_html_report.py"),
        ]
    )

    dated_html = OUTPUT_DIR / f"strategy_transition_report_{latest_date.date()}.html"

    if LATEST_HTML.exists():
        dated_html.write_text(LATEST_HTML.read_text())

    print(f"Saved latest HTML: {LATEST_HTML}")
    print(f"Saved dated HTML: {dated_html}")


if __name__ == "__main__":
    main()
