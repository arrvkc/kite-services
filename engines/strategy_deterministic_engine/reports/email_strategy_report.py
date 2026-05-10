from __future__ import annotations

import argparse
import os
import smtplib
from copy import copy
from datetime import date, datetime
from email.message import EmailMessage
from pathlib import Path

import pandas as pd
from sqlalchemy import text

from engines.strategy_deterministic_engine.db.postgres import get_engine, load_env_file


RAW_COLUMNS = [
    "SYMBOL", "PREV_LABEL", "LABEL", "LABEL_TRANSITION", "SCORE", "CONF", "STATE",
    "BULL5", "BEAR5", "FLAT5", "SIGNFLIP5", "MEAN3",
    "NEAR_DTE", "NEXT_DTE", "REGIME", "CANDIDATE_FAMILY",
    "STRATEGY_FAMILY", "CONTRACT_MONTH", "STRENGTH", "TOP_N",
    "RANK_ALL", "RANK_FAMILY", "TRANSITION_STATE", "REASONS",
]

RANKED_COLUMNS = [
    "SYMBOL", "LABEL", "SCORE", "CONF", "STRENGTH",
    "RANK_ALL", "REGIME", "CANDIDATE_FAMILY",
    "STRATEGY_FAMILY", "CONTRACT_MONTH",
]

NON_RANKED_COLUMNS = [
    "SYMBOL", "LABEL", "SCORE", "CONF", "STRENGTH",
    "REGIME", "CANDIDATE_FAMILY", "STRATEGY_FAMILY", "CONTRACT_MONTH",
]

TRANSITION_COLUMNS = [
    "SYMBOL", "PREV_LABEL", "LABEL", "LABEL_TRANSITION",
    "SCORE", "CONF", "STRENGTH", "RANK_ALL",
    "REGIME", "CANDIDATE_FAMILY", "STRATEGY_FAMILY", "CONTRACT_MONTH",
]


def read_df(engine, sql: str, params: dict) -> pd.DataFrame:
    with engine.connect() as conn:
        result = conn.execute(text(sql), params)
        rows = result.fetchall()
        columns = result.keys()
    return pd.DataFrame(rows, columns=columns)


def get_latest_run_date(engine) -> date:
    df = read_df(
        engine,
        """
        SELECT run_date
        FROM strategy_deterministic_engine_runs
        WHERE status = 'COMPLETED'
        ORDER BY run_date DESC
        LIMIT 1
        """,
        {},
    )
    if df.empty:
        raise RuntimeError("No completed strategy deterministic engine run found")
    return df.iloc[0]["run_date"]


def get_raw_results(engine, run_date: date) -> pd.DataFrame:
    return read_df(
        engine,
        """
        WITH latest_trend AS (
            SELECT
                r.symbol,
                th.trade_date,
                th.label,
                ROW_NUMBER() OVER (
                    PARTITION BY r.symbol
                    ORDER BY th.trade_date DESC
                ) AS rn
            FROM strategy_deterministic_engine_batch_results r
            JOIN trend_history_fo_universe th
              ON th.symbol = r.symbol
             AND th.trade_date <= r.run_date
            WHERE r.run_date = :run_date
        ),
        trend_labels AS (
            SELECT
                symbol,
                MAX(CASE WHEN rn = 1 THEN label END) AS current_label,
                MAX(CASE WHEN rn = 2 THEN label END) AS previous_label
            FROM latest_trend
            WHERE rn <= 2
            GROUP BY symbol
        )
        SELECT
            r.symbol AS "SYMBOL",
            tl.previous_label AS "PREV_LABEL",
            r.label AS "LABEL",
            CONCAT(tl.previous_label, ' - ', r.label) AS "LABEL_TRANSITION",
            ROUND(r.score::numeric, 2) AS "SCORE",
            ROUND(r.confidence::numeric, 4) AS "CONF",
            r.state AS "STATE",
            r.bull_count_5 AS "BULL5",
            r.bear_count_5 AS "BEAR5",
            r.flat_count_5 AS "FLAT5",
            r.sign_flip_count_5 AS "SIGNFLIP5",
            ROUND(r.mean_score_3::numeric, 2) AS "MEAN3",
            r.dte_near_month AS "NEAR_DTE",
            r.dte_next_month AS "NEXT_DTE",
            r.regime_bucket AS "REGIME",
            r.candidate_family AS "CANDIDATE_FAMILY",
            r.strategy_family AS "STRATEGY_FAMILY",
            r.contract_month_selection AS "CONTRACT_MONTH",
            r.final_strategy_strength AS "STRENGTH",
            CASE WHEN r.include_in_top_n THEN 'YES' ELSE 'NO' END AS "TOP_N",
            r.rank_overall AS "RANK_ALL",
            r.rank_in_family AS "RANK_FAMILY",
            r.strategy_transition_state AS "TRANSITION_STATE",
            r.reason_codes AS "REASONS"
        FROM strategy_deterministic_engine_batch_results r
        LEFT JOIN trend_labels tl
          ON tl.symbol = r.symbol
        WHERE r.run_date = :run_date
        ORDER BY r.symbol
        """,
        {"run_date": run_date},
    )


def style_sheet(writer, sheet_name: str) -> None:
    ws = writer.sheets[sheet_name]
    ws.freeze_panes = "A2"

    for cell in ws[1]:
        cell.font = copy(cell.font)
        cell.font = cell.font.copy(bold=True)
        cell.alignment = copy(cell.alignment)
        cell.alignment = cell.alignment.copy(horizontal="center")

    for column_cells in ws.columns:
        max_length = 0
        column_letter = column_cells[0].column_letter

        for cell in column_cells:
            value = "" if cell.value is None else str(cell.value)
            max_length = max(max_length, len(value))

        ws.column_dimensions[column_letter].width = min(max(max_length + 2, 10), 45)


def write_excel_report(raw_df: pd.DataFrame, run_date: date, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    sheet1 = (
        raw_df[raw_df["RANK_ALL"].notna()][RANKED_COLUMNS]
        .sort_values("RANK_ALL", na_position="last")
        .copy()
    )

    sheet2 = (
        raw_df[raw_df["RANK_ALL"].isna()][NON_RANKED_COLUMNS]
        .sort_values("STRENGTH", ascending=False, na_position="last")
        .copy()
    )

    sheet3 = raw_df[raw_df["LABEL"].astype(str).str.upper() == "UP"][RANKED_COLUMNS].copy()

    sheet4 = (
        raw_df[raw_df["STRATEGY_FAMILY"].astype(str).str.upper() == "IRON_CONDOR"][RANKED_COLUMNS]
        .sort_values("RANK_ALL", na_position="last")
        .copy()
    )

    sheet5 = (
        raw_df[raw_df["STRATEGY_FAMILY"].astype(str).str.upper() == "BULL_PUT_SPREAD"][RANKED_COLUMNS]
        .sort_values("RANK_ALL", na_position="last")
        .copy()
    )

    allowed_transitions = {
        "DOWN - FLAT",
        "FLAT - UP",
        "UP - FLAT",
        "FLAT - DOWN",
    }

    sheet6 = (
        raw_df[raw_df["LABEL_TRANSITION"].astype(str).str.upper().isin(allowed_transitions)][TRANSITION_COLUMNS]
        .sort_values("RANK_ALL", na_position="last")
        .copy()
    )

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        raw_df[RAW_COLUMNS].to_excel(writer, sheet_name="strategy_deterministic_engine_b", index=False)
        sheet1.to_excel(writer, sheet_name="Sheet1", index=False)
        sheet2.to_excel(writer, sheet_name="Sheet2", index=False)
        sheet3.to_excel(writer, sheet_name="Sheet3", index=False)
        sheet4.to_excel(writer, sheet_name="Sheet4", index=False)
        sheet5.to_excel(writer, sheet_name="Sheet5", index=False)
        sheet6.to_excel(writer, sheet_name="Sheet6_Label_Transitions", index=False)

        for sheet_name in writer.sheets:
            style_sheet(writer, sheet_name)


def send_email(attachment_path: Path, run_date: date) -> None:
    smtp_host = os.environ["SMTP_HOST"]
    smtp_port = int(os.environ.get("SMTP_PORT", "587"))
    smtp_user = os.environ["SMTP_USER"]
    smtp_password = os.environ["SMTP_PASSWORD"]
    mail_from = os.environ.get("REPORT_MAIL_FROM", smtp_user)
    mail_to = os.environ["REPORT_MAIL_TO"]

    msg = EmailMessage()
    msg["Subject"] = f"Strategy Deterministic Engine Report - {run_date}"
    msg["From"] = mail_from
    msg["To"] = mail_to

    msg.set_content(
        f"""Dear Chakravarthi,

Please find attached the Strategy Deterministic Engine report for {run_date}.

The workbook contains the exact report layout:
- strategy_deterministic_engine_b
- Sheet1
- Sheet2
- Sheet3
- Sheet4
- Sheet5
- Sheet6_Label_Transitions

Regards,
Kite Services
"""
    )

    msg.add_attachment(
        attachment_path.read_bytes(),
        maintype="application",
        subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=attachment_path.name,
    )

    with smtplib.SMTP(smtp_host, smtp_port) as server:
        server.starttls()
        server.login(smtp_user, smtp_password)
        server.send_message(msg)


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate and email strategy deterministic engine report.")
    parser.add_argument("--run-date", default=None, help="YYYY-MM-DD. Defaults to latest completed run.")
    parser.add_argument("--output-dir", default="logs/strategy_reports")
    parser.add_argument("--no-email", action="store_true")
    return parser


def main() -> None:
    load_env_file("/opt/kite_services/.env")
    args = build_argument_parser().parse_args()

    engine = get_engine()
    run_date = date.fromisoformat(args.run_date) if args.run_date else get_latest_run_date(engine)

    raw_df = get_raw_results(engine, run_date)

    if raw_df.empty:
        raise RuntimeError(f"No strategy results found for run_date={run_date}")

    output_path = Path(args.output_dir) / f"strategy_deterministic_engine_report_{run_date.strftime('%Y%m%d')}.xlsx"

    write_excel_report(raw_df, run_date, output_path)
    print(f"Generated report: {output_path}")

    if not args.no_email:
        send_email(output_path, run_date)
        print(f"Emailed report: {output_path}")


if __name__ == "__main__":
    main()
