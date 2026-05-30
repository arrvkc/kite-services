import argparse
import csv
import os
import smtplib
from email.message import EmailMessage
from pathlib import Path
from time import strftime

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

    new_text = after.replace(before, "").strip()
    new_lines = [line for line in new_text.splitlines() if line.strip()]

    success = [line for line in new_lines if ",SUCCESS," in line]
    failed = [
        line
        for line in new_lines
        if ",FAILED," in line or ",UNKNOWN_FINAL_STATE," in line
    ]

    final_status = "SUCCESS" if not failed else "PARTIAL_FAILURE"

    html = f"""
    <html>
    <body>
    <h2>EAJEE Zerodha Token Refresh Report</h2>
    <p><b>Status:</b> {final_status}</p>
    <p><b>Time:</b> {strftime("%Y-%m-%d %H:%M:%S")}</p>
    <p><b>Total:</b> {len(new_lines)}</p>
    <p><b>Success:</b> {len(success)}</p>
    <p><b>Failed:</b> {len(failed)}</p>
    <pre>{chr(10).join(new_lines)}</pre>
    </body>
    </html>
    """

    subject = f"[EAJEE] Zerodha Token Refresh - {final_status}"

    if args.email_to:
        send_email(args.email_to, subject, html)
        print(f"Email sent to {args.email_to}")

    print(f"FINAL_STATUS={final_status}")


if __name__ == "__main__":
    main()
