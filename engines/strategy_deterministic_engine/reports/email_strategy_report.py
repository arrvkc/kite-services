from __future__ import annotations

import argparse
import os
import smtplib
from datetime import date, datetime
from email.message import EmailMessage
from pathlib import Path

import pandas as pd
from sqlalchemy import text

from engines.strategy_deterministic_engine.db.postgres import get_engine, load_env_file


REPORT_COLUMNS = [
    "symbol",
    "label",
    "score",
    "confidence",
    "state",
    "strategy_family",
    "contract_month_selection",
    "final_strategy_strength",
    "include_in_top_n",
    "rank_overall",
    "rank_in_family",
    "reason_codes",
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


def get_base_results(engine, run_date: date) -> pd.DataFrame:
    df = read_df(
        engine,
        f"""
        SELECT {", ".join(REPORT_COLUMNS)}
        FROM strategy_deterministic_engine_batch_results
        WHERE run_date = :run_date
        ORDER BY rank_overall NULLS LAST, symbol
        """,
        {"run_date": run_date},
    )
    return df


def write_excel_report(df: pd.DataFrame, run_date: date, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    sheet1_top_n = df[df["include_in_top_n"] == True].copy()
    sheet2_non_top_n = df[df["include_in_top_n"] != True].copy()
    sheet3_up = df[df["label"].astype(str).str.upper() == "UP"].copy()
    sheet4_iron_condor = df[df["strategy_family"].astype(str).str.upper() == "IRON_CONDOR"].copy()
    sheet5_bull_put = df[df["strategy_family"].astype(str).str.upper() == "BULL_PUT_SPREAD"].copy()

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Raw_Output", index=False)
        sheet1_top_n.to_excel(writer, sheet_name="Sheet1_Top_N", index=False)
        sheet2_non_top_n.to_excel(writer, sheet_name="Sheet2_Non_Top_N", index=False)
        sheet3_up.to_excel(writer, sheet_name="Sheet3_UP", index=False)
        sheet4_iron_condor.to_excel(writer, sheet_name="Sheet4_Iron_Condor", index=False)
        sheet5_bull_put.to_excel(writer, sheet_name="Sheet5_Bull_Put", index=False)

        workbook = writer.book

        for sheet_name in writer.sheets:
            ws = writer.sheets[sheet_name]
            ws.freeze_panes = "A2"

            for cell in ws[1]:
                cell.font = cell.font.copy(bold=True)
                cell.alignment = cell.alignment.copy(horizontal="center")

            for column_cells in ws.columns:
                max_length = 0
                column_letter = column_cells[0].column_letter
                for cell in column_cells:
                    value = "" if cell.value is None else str(cell.value)
                    max_length = max(max_length, len(value))
                ws.column_dimensions[column_letter].width = min(max(max_length + 2, 10), 45)

        meta = workbook.create_sheet("Summary", 0)
        meta["A1"] = "Strategy Deterministic Engine Report"
        meta["A2"] = "Run Date"
        meta["B2"] = str(run_date)
        meta["A3"] = "Generated At"
        meta["B3"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        meta["A5"] = "Raw Output Rows"
        meta["B5"] = len(df)
        meta["A6"] = "Top N Rows"
        meta["B6"] = len(sheet1_top_n)
        meta["A7"] = "Non Top N Rows"
        meta["B7"] = len(sheet2_non_top_n)
        meta["A8"] = "UP Rows"
        meta["B8"] = len(sheet3_up)
        meta["A9"] = "Iron Condor Rows"
        meta["B9"] = len(sheet4_iron_condor)
        meta["A10"] = "Bull Put Spread Rows"
        meta["B10"] = len(sheet5_bull_put)

        for column in ["A", "B"]:
            meta.column_dimensions[column].width = 35


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

Attached workbook includes:
- Raw_Output
- Sheet1_Top_N
- Sheet2_Non_Top_N
- Sheet3_UP
- Sheet4_Iron_Condor
- Sheet5_Bull_Put

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
    parser.add_argument("--no-email", action="store_true", help="Generate XLSX only, do not email.")
    return parser


def main() -> None:
    load_env_file("/opt/kite_services/.env")
    args = build_argument_parser().parse_args()

    engine = get_engine()

    run_date = date.fromisoformat(args.run_date) if args.run_date else get_latest_run_date(engine)
    df = get_base_results(engine, run_date)

    if df.empty:
        raise RuntimeError(f"No strategy results found for run_date={run_date}")

    output_path = Path(args.output_dir) / f"strategy_deterministic_engine_report_{run_date.strftime('%Y%m%d')}.xlsx"

    write_excel_report(df, run_date, output_path)
    print(f"Generated report: {output_path}")

    if not args.no_email:
        send_email(output_path, run_date)
        print(f"Emailed report: {output_path}")


if __name__ == "__main__":
    main()
