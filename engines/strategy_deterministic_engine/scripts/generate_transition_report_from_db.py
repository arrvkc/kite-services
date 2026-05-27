from __future__ import annotations

import argparse
import subprocess
from io import StringIO
from pathlib import Path

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[3]
OUTPUT_DIR = REPO_ROOT / "data" / "strategy_transition_reports"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

INPUT_CSV = OUTPUT_DIR / "strategy_engine_batch_output_latest_from_db.csv"
TRANSITION_CSV = OUTPUT_DIR / "strategy_transition_scanner_latest.csv"
LATEST_HTML = OUTPUT_DIR / "strategy_transition_report_latest.html"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate strategy transition dashboard from DB-backed strategy batch results."
    )
    parser.add_argument("--run-date", default="", help="Optional run date, e.g. 2026-05-26")
    parser.add_argument("--user-id", default="OMK569", help="Kite user id for price/volume enrichment")
    return parser.parse_args()


def query_csv(sql: str) -> str:
    cmd = [
        "docker",
        "exec",
        "-i",
        "postgres",
        "psql",
        "-U",
        "postgres",
        "-d",
        "atms",
        "-c",
        f"COPY ({sql}) TO STDOUT WITH CSV HEADER",
    ]
    return subprocess.check_output(cmd, text=True)


def export_strategy_results_from_db() -> None:
    sql = """
    SELECT
        run_date,
        symbol,
        label,
        score,
        confidence,
        state,
        bull_count_5,
        bear_count_5,
        flat_count_5,
        sign_flip_count_5,
        mean_score_3,
        dte_near_month,
        dte_next_month,
        regime_bucket,
        candidate_family,
        strategy_family,
        contract_month_selection,
        final_strategy_strength,
        include_in_top_n,
        rank_overall,
        rank_in_family,
        strategy_transition_state,
        reason_codes
    FROM strategy_deterministic_engine_batch_results
    ORDER BY symbol, run_date
    """

    csv_text = query_csv(sql)
    INPUT_CSV.write_text(csv_text)
    print(f"Saved DB export CSV: {INPUT_CSV}")


def latest_run_date_from_export() -> str:
    df = pd.read_csv(INPUT_CSV, usecols=["run_date"])
    return str(pd.to_datetime(df["run_date"]).max().date())


def main() -> None:
    args = parse_args()

    export_strategy_results_from_db()

    run_date = args.run_date or latest_run_date_from_export()

    scanner_cmd = [
        "python",
        str(REPO_ROOT / "engines" / "strategy_deterministic_engine" / "scripts" / "scan_transitions_from_csv.py"),
        "--input-csv",
        str(INPUT_CSV),
        "--output-csv",
        str(TRANSITION_CSV),
        "--run-date",
        run_date,
    ]

    if args.user_id:
        scanner_cmd.extend(["--user-id", args.user_id])

    print("Running transition scanner:")
    print(" ".join(scanner_cmd))
    subprocess.check_call(scanner_cmd)

    html_cmd = [
        "python",
        str(REPO_ROOT / "engines" / "strategy_deterministic_engine" / "scripts" / "generate_transition_html_report.py"),
    ]

    print("Running HTML generator:")
    print(" ".join(html_cmd))
    subprocess.check_call(html_cmd)

    dated_html = OUTPUT_DIR / f"strategy_transition_report_{run_date}.html"

    if LATEST_HTML.exists():
        dated_html.write_text(LATEST_HTML.read_text())

    print(f"Saved latest HTML: {LATEST_HTML}")
    print(f"Saved dated HTML: {dated_html}")


if __name__ == "__main__":
    main()
