#!/usr/bin/env python3
import argparse
import os
import smtplib
import ssl
import socket
from datetime import datetime
from email.message import EmailMessage
from pathlib import Path


def load_dotenv(path):
    p = Path(path)
    if not p.exists():
        return

    for raw in p.read_text(errors="ignore").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")

        if key and key not in os.environ:
            os.environ[key] = value


def first_env(*names, default=None):
    for name in names:
        value = os.environ.get(name)
        if value is not None and str(value).strip() != "":
            return str(value).strip()
    return default


def bool_env(*names, default=False):
    value = first_env(*names)
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def split_emails(value):
    if not value:
        return []
    value = value.replace(";", ",")
    return [x.strip() for x in value.split(",") if x.strip()]


def tail_file(path, lines):
    if not path:
        return "<not provided>"

    p = Path(path)
    if not p.exists():
        return f"<file not found: {path}>"

    try:
        data = p.read_text(errors="ignore").splitlines()
        return "\n".join(data[-lines:])
    except Exception as exc:
        return f"<unable to read file: {path}; error={exc}>"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--status", required=True)
    parser.add_argument("--exit-code", required=True)
    parser.add_argument("--started-at", required=True)
    parser.add_argument("--ended-at", required=True)
    parser.add_argument("--duration-seconds", required=True)
    parser.add_argument("--run-log", required=True)
    parser.add_argument("--master-log", required=True)
    parser.add_argument("--atms-cli-log", required=True)
    parser.add_argument("--container-log", required=True)
    parser.add_argument("--container-name", required=True)
    parser.add_argument("--cmd-path", required=True)
    parser.add_argument("--env-file", default="/opt/kite_services/.env")
    args = parser.parse_args()

    load_dotenv(args.env_file)

    smtp_host = first_env("FETCH52W_SMTP_HOST", "SMTP_HOST", "SMTP_SERVER", "MAIL_SERVER")
    smtp_port = int(first_env("FETCH52W_SMTP_PORT", "SMTP_PORT", "MAIL_PORT", default="587"))

    smtp_user = first_env("FETCH52W_SMTP_USER", "SMTP_USER", "SMTP_USERNAME", "MAIL_USERNAME")
    smtp_password = first_env("FETCH52W_SMTP_PASSWORD", "SMTP_PASSWORD", "MAIL_PASSWORD")

    smtp_ssl = bool_env("FETCH52W_SMTP_USE_SSL", "SMTP_USE_SSL", "MAIL_USE_SSL", default=False)
    smtp_tls = bool_env("FETCH52W_SMTP_USE_TLS", "SMTP_USE_TLS", "MAIL_USE_TLS", default=not smtp_ssl)

    sender = first_env(
        "FETCH52W_EMAIL_FROM",
        "SMTP_FROM",
        "MAIL_DEFAULT_SENDER",
        "MAIL_USERNAME",
        "SMTP_USER",
        default=smtp_user,
    )

    recipients_raw = first_env(
        "FETCH52W_EMAIL_TO",
        "REPORT_MAIL_TO",
        "FETCH52W_ALERT_EMAIL_TO",
        "ALERT_EMAIL_TO",
        "EMAIL_TO",
        "REPORT_EMAIL_TO",
        "STRATEGY_EMAIL_TO",
        "MAIL_RECIPIENTS",
    )

    recipients = split_emails(recipients_raw)

    missing = []
    if not smtp_host:
        missing.append("MAIL_SERVER or SMTP_HOST")
    if not sender:
        missing.append("MAIL_DEFAULT_SENDER or SMTP_FROM or MAIL_USERNAME")
    if not recipients:
        missing.append("FETCH52W_EMAIL_TO or REPORT_MAIL_TO or ALERT_EMAIL_TO or REPORT_EMAIL_TO or STRATEGY_EMAIL_TO")

    if missing:
        raise SystemExit("Missing email config in .env: " + ", ".join(missing))

    status = args.status.upper()
    subject = f"[ATMS FETCH52W] {status} - NSE 52W fetch - {datetime.now().strftime('%Y-%m-%d %H:%M')}"

    body = f"""ATMS Fetch52W cron notification.

Status           : {status}
Exit Code        : {args.exit_code}
Host             : {socket.gethostname()}
Container        : {args.container_name}
Command Path     : {args.cmd_path}
Started At       : {args.started_at}
Ended At         : {args.ended_at}
Duration Seconds : {args.duration_seconds}

Run Log          : {args.run_log}
Master Log       : {args.master_log}
ATMS CLI Log     : {args.atms_cli_log}
Container Log    : {args.container_log}

============================================================
RUN LOG TAIL
============================================================
{tail_file(args.run_log, 140)}

============================================================
ATMS CLI LOG TAIL
============================================================
{tail_file(args.atms_cli_log, 140)}

============================================================
MASTER LOG TAIL
============================================================
{tail_file(args.master_log, 80)}
"""

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = ", ".join(recipients)
    msg.set_content(body)

    if smtp_ssl:
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL(smtp_host, smtp_port, context=context, timeout=30) as server:
            if smtp_user and smtp_password:
                server.login(smtp_user, smtp_password)
            server.send_message(msg)
    else:
        with smtplib.SMTP(smtp_host, smtp_port, timeout=30) as server:
            server.ehlo()
            if smtp_tls:
                context = ssl.create_default_context()
                server.starttls(context=context)
                server.ehlo()
            if smtp_user and smtp_password:
                server.login(smtp_user, smtp_password)
            server.send_message(msg)

    print(f"EMAIL_SENT to={','.join(recipients)} subject={subject}")


if __name__ == "__main__":
    main()
