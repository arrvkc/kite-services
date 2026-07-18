import argparse
import csv
import os
import smtplib
from email.message import EmailMessage
from datetime import datetime
from zoneinfo import ZoneInfo

from .config import BASE_DIR


LOG_DIR = BASE_DIR / "logs" / "eajee_web_automation"
RESULTS_CSV = LOG_DIR / "refresh_token_results.csv"


def send_email(to_email, subject, html_body):
    if not to_email:
        return

    smtp_host = os.getenv("SMTP_HOST")
    smtp_port = int(os.getenv("SMTP_PORT", "587"))
    smtp_user = os.getenv("SMTP_USER")
    smtp_password = os.getenv("SMTP_PASSWORD")
    mail_from = os.getenv("REPORT_MAIL_FROM") or smtp_user

    if not smtp_host or not smtp_user or not smtp_password or not mail_from:
        raise RuntimeError("Missing SMTP config")

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = mail_from
    msg["To"] = to_email

    msg.set_content("EAJEE Zerodha token refresh report.")
    msg.add_alternative(html_body, subtype="html")

    with smtplib.SMTP(smtp_host, smtp_port) as smtp:
        smtp.starttls()
        smtp.login(smtp_user, smtp_password)
        smtp.send_message(msg)


def run_single_user(user_id, headless):
    from .refresh_user_token import main as single_main
    import sys

    old_argv = sys.argv[:]

    try:
        sys.argv = ["refresh_user_token", user_id]

        if headless:
            sys.argv.append("--headless")

        single_main()

    finally:
        sys.argv = old_argv


def parse_new_rows(before, after):
    new_text = after.replace(before, "").strip()
    lines = [
        line
        for line in new_text.splitlines()
        if line.strip() and not line.startswith("timestamp,")
    ]

    parsed = []

    for line in lines:
        parts = line.split(",")

        parsed.append(
            {
                "raw": line,
                "timestamp": parts[0] if len(parts) > 0 else "",
                "user_id": parts[1] if len(parts) > 1 else "",
                "mode": parts[2] if len(parts) > 2 else "",
                "status": parts[3] if len(parts) > 3 else "",
                "stage": parts[4] if len(parts) > 4 else "",
                "refresh_url": parts[5] if len(parts) > 5 else "",
                "final_url": parts[6] if len(parts) > 6 else "",
                "error": parts[7] if len(parts) > 7 else "",
                "run_dir": parts[9] if len(parts) > 9 else "",
                "zip_file": parts[10] if len(parts) > 10 else "",
            }
        )

    return parsed


def build_html_report(rows, final_status):
    success_count = sum(1 for r in rows if r["status"] == "SUCCESS")
    failed_rows = [r for r in rows if r["status"] != "SUCCESS"]

    table_rows = []

    for r in rows:
        status_color = "#0a7d28" if r["status"] == "SUCCESS" else "#b00020"

        table_rows.append(
            f"""
            <tr>
                <td>{r['timestamp']}</td>
                <td><b>{r['user_id']}</b></td>
                <td>{r['mode']}</td>
                <td style="color:{status_color}; font-weight:bold;">{r['status']}</td>
                <td>{r['stage']}</td>
                <td>{r['error'] or '-'}</td>
                <td>{r['final_url']}</td>
                <td>{r['run_dir']}</td>
            </tr>
            """
        )

    failure_block = ""

    if failed_rows:
        failure_items = "".join(
            f"<li><b>{r['user_id']}</b> failed at <b>{r['stage']}</b>: {r['error']}</li>"
            for r in failed_rows
        )

        failure_block = f"""
        <h3 style="color:#b00020;">Failures Requiring Attention</h3>
        <ul>{failure_items}</ul>
        """

    return f"""
    <html>
    <body style="font-family: Arial, sans-serif;">
        <h2>EAJEE Zerodha Token Refresh Report</h2>

        <p><b>Status:</b> {final_status}</p>
        <p><b>Time:</b> {datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%Y-%m-%d %H:%M:%S IST")}</p>
        <p><b>Total:</b> {len(rows)}</p>
        <p><b>Success:</b> {success_count}</p>
        <p><b>Failed:</b> {len(failed_rows)}</p>

        {failure_block}

        <table border="1" cellspacing="0" cellpadding="6" style="border-collapse: collapse;">
            <thead>
                <tr>
                    <th>Timestamp</th>
                    <th>User</th>
                    <th>Mode</th>
                    <th>Status</th>
                    <th>Stage</th>
                    <th>Error</th>
                    <th>Final URL</th>
                    <th>Run Directory</th>
                </tr>
            </thead>
            <tbody>
                {''.join(table_rows)}
            </tbody>
        </table>
    </body>
    </html>
    """


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--users", nargs="+", required=True)
    parser.add_argument("--headless", action="store_true")
    parser.add_argument("--email-to", default="")
    args = parser.parse_args()

    before = RESULTS_CSV.read_text() if RESULTS_CSV.exists() else ""

    for user_id in args.users:
        print(f"Refreshing {user_id}")
        run_single_user(user_id, args.headless)

    after = RESULTS_CSV.read_text() if RESULTS_CSV.exists() else ""

    rows = parse_new_rows(before, after)

    failed_rows = [r for r in rows if r["status"] != "SUCCESS"]

    final_status = "SUCCESS" if not failed_rows else "PARTIAL_FAILURE"

    html = build_html_report(rows, final_status)

    subject = f"[EAJEE] Zerodha Token Refresh - {final_status}"

    if args.email_to:
        send_email(args.email_to, subject, html)
        print(f"Email sent to {args.email_to}")

    print(f"FINAL_STATUS={final_status}")


if __name__ == "__main__":
    main()
