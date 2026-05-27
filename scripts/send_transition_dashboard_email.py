from __future__ import annotations

import html
import os
import smtplib
from email.message import EmailMessage
from pathlib import Path

import pandas as pd


BASE = Path("/opt/kite_services/data/strategy_transition_reports")
CSV_PATH = BASE / "strategy_transition_scanner_latest.csv"

PUBLIC_BASE_URL = "https://eajee.in/reports/strategy-transition"


def main() -> None:
    if not CSV_PATH.exists():
        raise SystemExit(f"Missing CSV: {CSV_PATH}")

    df = pd.read_csv(CSV_PATH)

    run_date = str(df["run_date"].max()) if not df.empty and "run_date" in df.columns else "unknown"
    total = len(df)

    high_priority = 0
    very_high = 0
    high = 0

    if "signal_quality" in df.columns:
        very_high = int((df["signal_quality"] == "Very High").sum())
        high = int((df["signal_quality"] == "High").sum())
        high_priority = very_high + high

    top_rows = df.head(10)

    rows_html = ""
    for _, r in top_rows.iterrows():
        rows_html += f"""
        <tr>
            <td>{html.escape(str(r.get("symbol", "")))}</td>
            <td>{html.escape(str(r.get("change_types", "")))}</td>
            <td>{html.escape(str(r.get("current_strategy_family", "")))}</td>
            <td>{html.escape(str(r.get("current_regime_bucket", "")))}</td>
            <td>{html.escape(str(r.get("current_strength", "")))}</td>
            <td>{html.escape(str(r.get("price_change_pct", "")))}</td>
            <td>{html.escape(str(r.get("volume_ratio_20d", "")))}</td>
        </tr>
        """

    latest_url = f"{PUBLIC_BASE_URL}/latest.html"
    dated_url = f"{PUBLIC_BASE_URL}/{run_date}.html"

    body = f"""
    <html>
    <body style="font-family: Arial, sans-serif; color:#172033;">
        <h2>Strategy Transition Dashboard Ready</h2>

        <p><b>Run date:</b> {html.escape(run_date)}</p>

        <table style="border-collapse:collapse;">
            <tr>
                <td style="padding:10px;border:1px solid #ddd;"><b>Total transitions</b></td>
                <td style="padding:10px;border:1px solid #ddd;">{total}</td>
            </tr>
            <tr>
                <td style="padding:10px;border:1px solid #ddd;"><b>Very High</b></td>
                <td style="padding:10px;border:1px solid #ddd;">{very_high}</td>
            </tr>
            <tr>
                <td style="padding:10px;border:1px solid #ddd;"><b>High</b></td>
                <td style="padding:10px;border:1px solid #ddd;">{high}</td>
            </tr>
            <tr>
                <td style="padding:10px;border:1px solid #ddd;"><b>High Priority Total</b></td>
                <td style="padding:10px;border:1px solid #ddd;">{high_priority}</td>
            </tr>
        </table>

        <p>
            <a href="{latest_url}">Open Latest Dashboard</a><br>
            <a href="{dated_url}">Open Dated Dashboard</a>
        </p>

        <h3>Top Transition Rows</h3>
        <table style="border-collapse:collapse;width:100%;">
            <thead>
                <tr>
                    <th style="border:1px solid #ddd;padding:7px;">Symbol</th>
                    <th style="border:1px solid #ddd;padding:7px;">Change</th>
                    <th style="border:1px solid #ddd;padding:7px;">Strategy</th>
                    <th style="border:1px solid #ddd;padding:7px;">Regime</th>
                    <th style="border:1px solid #ddd;padding:7px;">Strength</th>
                    <th style="border:1px solid #ddd;padding:7px;">Price %</th>
                    <th style="border:1px solid #ddd;padding:7px;">Volume Ratio</th>
                </tr>
            </thead>
            <tbody>{rows_html}</tbody>
        </table>

        <p style="color:#64748b;font-size:12px;">
            Decision-support report only. Not an automated trade instruction.
        </p>
    </body>
    </html>
    """

    to_addr = os.getenv("REPORT_MAIL_TO", "").strip()
    from_addr = os.getenv("REPORT_MAIL_FROM", "").strip()
    smtp_host = os.getenv("SMTP_HOST", "").strip()
    smtp_port = int(os.getenv("SMTP_PORT", "587"))
    smtp_user = os.getenv("SMTP_USER", "").strip()
    smtp_password = os.getenv("SMTP_PASSWORD", "").strip()

    if not all([to_addr, from_addr, smtp_host, smtp_user, smtp_password]):
        raise SystemExit("Missing REPORT_MAIL_TO/REPORT_MAIL_FROM or SMTP_* env vars.")

    msg = EmailMessage()
    msg["Subject"] = f"Strategy Transition Dashboard Ready - {run_date}"
    msg["From"] = from_addr
    msg["To"] = to_addr
    msg.set_content(f"Strategy Transition Dashboard Ready: {latest_url}")
    msg.add_alternative(body, subtype="html")

    with smtplib.SMTP(smtp_host, smtp_port) as server:
        server.starttls()
        server.login(smtp_user, smtp_password)
        server.send_message(msg)

    print(f"Transition dashboard email sent to {to_addr}")


if __name__ == "__main__":
    main()
