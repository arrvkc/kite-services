#!/bin/bash
set -euo pipefail

REPORT_FILE="/opt/kite_services/setup_fetch52w_docker_exec_cron_with_email_report.txt"

{
  echo "=================================================="
  echo "SETUP FETCH52W DOCKER EXEC CRON WITH EMAIL REPORT"
  echo "=================================================="
  echo "STARTED: $(date '+%Y-%m-%d %H:%M:%S')"
  echo "HOST   : $(hostname)"
  echo ""
} > "$REPORT_FILE"

log() {
  echo "$1" | tee -a "$REPORT_FILE"
}

run_report() {
  local title="$1"
  shift

  {
    echo ""
    echo "--------------------------------------------------"
    echo "$title"
    echo "--------------------------------------------------"
    echo "COMMAND: $*"
    echo "--------------------------------------------------"
    "$@"
    echo "EXIT_CODE: $?"
  } >> "$REPORT_FILE" 2>&1
}

KITE_APP_DIR="/opt/kite_services"
KITE_ENV_FILE="$KITE_APP_DIR/.env"
KITE_VENV_PYTHON="$KITE_APP_DIR/venv/bin/python"

ATMS_CONTAINER="atms"

FETCH52W_RUNNER="$KITE_APP_DIR/run_fetch52w_atms_cron.sh"
FETCH52W_EMAIL_SCRIPT="$KITE_APP_DIR/send_fetch52w_atms_email.py"
FETCH52W_LOG_DIR="$KITE_APP_DIR/logs/fetch52w_atms"

log "STEP 1: Validate base paths"

if [ ! -d "$KITE_APP_DIR" ]; then
  log "ERROR: $KITE_APP_DIR not found."
  exit 1
fi

if [ ! -f "$KITE_ENV_FILE" ]; then
  log "ERROR: $KITE_ENV_FILE not found."
  exit 1
fi

if [ ! -x "$KITE_VENV_PYTHON" ]; then
  log "ERROR: $KITE_VENV_PYTHON not found or not executable."
  exit 1
fi

mkdir -p "$FETCH52W_LOG_DIR"

log "OK: KITE_APP_DIR=$KITE_APP_DIR"
log "OK: KITE_ENV_FILE=$KITE_ENV_FILE"
log "OK: KITE_VENV_PYTHON=$KITE_VENV_PYTHON"
log "OK: FETCH52W_LOG_DIR=$FETCH52W_LOG_DIR"

log ""
log "STEP 2: Validate Docker and ATMS container"

if ! command -v docker >/dev/null 2>&1; then
  log "ERROR: docker command not found."
  exit 1
fi

if ! docker ps --format '{{.Names}}' | grep -qx "$ATMS_CONTAINER"; then
  log "ERROR: running container '$ATMS_CONTAINER' not found."
  log "Current containers:"
  docker ps >> "$REPORT_FILE" 2>&1 || true
  exit 1
fi

run_report "docker ps" docker ps

log "OK: Found running container: $ATMS_CONTAINER"

log ""
log "STEP 3: Find and validate cmd_atms.py inside container"

CMD_PATH="$(docker exec "$ATMS_CONTAINER" sh -lc 'find . -name cmd_atms.py -print | head -n 1' | tr -d '\r')"

if [ -z "$CMD_PATH" ]; then
  log "ERROR: cmd_atms.py not found inside $ATMS_CONTAINER."
  docker exec "$ATMS_CONTAINER" sh -lc 'pwd; find . -maxdepth 5 -type f -name "*.py" | head -n 80' >> "$REPORT_FILE" 2>&1 || true
  exit 1
fi

log "CMD_PATH_INSIDE_CONTAINER=$CMD_PATH"

HELP_CHECK="$(docker exec "$ATMS_CONTAINER" sh -lc "python $CMD_PATH --help | grep -i fetch-52w-cmd || true" | tr -d '\r')"

if ! echo "$HELP_CHECK" | grep -q 'fetch-52w-cmd'; then
  log "ERROR: fetch-52w-cmd not found in cmd_atms.py help."
  log "HELP_CHECK=$HELP_CHECK"
  log ""
  log "This means the server container does not yet have the patched cmd_atms.py."
  exit 1
fi

log "OK: $HELP_CHECK"

log ""
log "STEP 4: Create email helper script"

cat > "$FETCH52W_EMAIL_SCRIPT" <<'PYEOF'
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
        missing.append("FETCH52W_EMAIL_TO or ALERT_EMAIL_TO or REPORT_EMAIL_TO or STRATEGY_EMAIL_TO")

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
PYEOF

chmod +x "$FETCH52W_EMAIL_SCRIPT"

"$KITE_VENV_PYTHON" -m py_compile "$FETCH52W_EMAIL_SCRIPT" >> "$REPORT_FILE" 2>&1

log "EMAIL_SCRIPT_CREATED=$FETCH52W_EMAIL_SCRIPT"
log "EMAIL_SCRIPT_COMPILE=OK"

log ""
log "STEP 5: Create fetch52w runner script using docker exec"

cat > "$FETCH52W_RUNNER" <<RUNEOF
#!/bin/bash
set -euo pipefail

KITE_APP_DIR="$KITE_APP_DIR"
LOG_DIR="$FETCH52W_LOG_DIR"
EMAIL_SCRIPT="$FETCH52W_EMAIL_SCRIPT"
ATMS_CONTAINER="$ATMS_CONTAINER"
CMD_PATH="$CMD_PATH"

RUN_DATE="\$(date '+%Y-%m-%d')"
RUN_TS="\$(date '+%Y-%m-%d_%H-%M-%S')"
START_EPOCH="\$(date +%s)"
STARTED_AT="\$(date '+%Y-%m-%d %H:%M:%S')"

mkdir -p "\$LOG_DIR"

cd "\$KITE_APP_DIR"

set -a
. "\$KITE_APP_DIR/.env"
set +a

MASTER_LOG="\$LOG_DIR/fetch52w_\$RUN_DATE.log"
RUN_LOG="\$LOG_DIR/fetch52w_run_\$RUN_TS.log"
EMAIL_LOG="\$LOG_DIR/fetch52w_email_\$RUN_TS.log"
CONTAINER_LOG="\$LOG_DIR/fetch52w_container_\$RUN_TS.log"

STATUS="SUCCESS"
EXIT_CODE=0

echo "==================================================" >> "\$MASTER_LOG"
echo "FETCH52W ATMS CRON STARTED: \$RUN_TS" >> "\$MASTER_LOG"
echo "CONTAINER: \$ATMS_CONTAINER" >> "\$MASTER_LOG"
echo "CMD_PATH: \$CMD_PATH" >> "\$MASTER_LOG"
echo "RUN_LOG: \$RUN_LOG" >> "\$MASTER_LOG"
echo "CONTAINER_LOG: \$CONTAINER_LOG" >> "\$MASTER_LOG"
echo "==================================================" >> "\$MASTER_LOG"

{
  echo ""
  echo "--------------------------------------------------"
  echo "FETCH52W COMMAND START"
  echo "START: \$(date '+%Y-%m-%d %H:%M:%S')"
  echo "CONTAINER: \$ATMS_CONTAINER"
  echo "CMD_PATH: \$CMD_PATH"
  echo "--------------------------------------------------"

  set +e
  docker exec "\$ATMS_CONTAINER" sh -lc "mkdir -p ./logs/fetch_52w && ATMS_FETCH52W_LOG_DIR=./logs/fetch_52w python \$CMD_PATH fetch-52w-cmd --file-for BOTH" > "\$CONTAINER_LOG" 2>&1
  EXIT_CODE=\$?
  set -e

  if [ "\$EXIT_CODE" -ne 0 ]; then
    STATUS="FAILED"
    echo "FETCH52W COMMAND FAILED"
  else
    echo "FETCH52W COMMAND COMPLETE"
  fi

  echo "END: \$(date '+%Y-%m-%d %H:%M:%S')"
  echo "STATUS: \$STATUS"
  echo "EXIT_CODE: \$EXIT_CODE"
  echo ""
  echo "CONTAINER OUTPUT:"
  cat "\$CONTAINER_LOG" || true
  echo "--------------------------------------------------"

} >> "\$RUN_LOG" 2>&1

ENDED_AT="\$(date '+%Y-%m-%d %H:%M:%S')"
END_EPOCH="\$(date +%s)"
DURATION_SECONDS="\$((END_EPOCH - START_EPOCH))"

ATMS_CLI_LOG_IN_CONTAINER="./logs/fetch_52w/fetch_52w_\$RUN_DATE.log"
ATMS_CLI_LOG_ON_HOST="\$LOG_DIR/atms_cli_fetch_52w_\$RUN_TS.log"

set +e
docker exec "\$ATMS_CONTAINER" sh -lc "cat \$ATMS_CLI_LOG_IN_CONTAINER 2>/dev/null || true" > "\$ATMS_CLI_LOG_ON_HOST" 2>&1
set -e

echo "FETCH52W ATMS CRON FINISHED: \$(date '+%Y-%m-%d_%H-%M-%S')" >> "\$MASTER_LOG"
echo "STATUS: \$STATUS" >> "\$MASTER_LOG"
echo "EXIT_CODE: \$EXIT_CODE" >> "\$MASTER_LOG"
echo "DURATION_SECONDS: \$DURATION_SECONDS" >> "\$MASTER_LOG"
echo "RUN_LOG: \$RUN_LOG" >> "\$MASTER_LOG"
echo "EMAIL_LOG: \$EMAIL_LOG" >> "\$MASTER_LOG"
echo "CONTAINER_LOG: \$CONTAINER_LOG" >> "\$MASTER_LOG"
echo "ATMS_CLI_LOG_ON_HOST: \$ATMS_CLI_LOG_ON_HOST" >> "\$MASTER_LOG"
echo "==================================================" >> "\$MASTER_LOG"

set +e
"\$KITE_APP_DIR/venv/bin/python" "\$EMAIL_SCRIPT" \\
  --status "\$STATUS" \\
  --exit-code "\$EXIT_CODE" \\
  --started-at "\$STARTED_AT" \\
  --ended-at "\$ENDED_AT" \\
  --duration-seconds "\$DURATION_SECONDS" \\
  --run-log "\$RUN_LOG" \\
  --master-log "\$MASTER_LOG" \\
  --atms-cli-log "\$ATMS_CLI_LOG_ON_HOST" \\
  --container-log "\$CONTAINER_LOG" \\
  --container-name "\$ATMS_CONTAINER" \\
  --cmd-path "\$CMD_PATH" \\
  --env-file "\$KITE_APP_DIR/.env" \\
  >> "\$EMAIL_LOG" 2>&1

EMAIL_EXIT=\$?
set -e

echo "EMAIL_EXIT_CODE: \$EMAIL_EXIT" >> "\$MASTER_LOG"

if [ "\$EMAIL_EXIT" -ne 0 ]; then
  echo "EMAIL_STATUS: FAILED" >> "\$MASTER_LOG"
else
  echo "EMAIL_STATUS: SENT" >> "\$MASTER_LOG"
fi

exit "\$EXIT_CODE"
RUNEOF

chmod +x "$FETCH52W_RUNNER"

log "RUNNER_CREATED=$FETCH52W_RUNNER"

log ""
log "STEP 6: Show email-related .env keys without secrets"

grep -E '^(FETCH52W_|SMTP_|MAIL_|ALERT_EMAIL_TO|EMAIL_TO|REPORT_EMAIL_TO|STRATEGY_EMAIL_TO)' "$KITE_ENV_FILE" \
  | sed -E 's/(PASSWORD|SECRET|TOKEN|KEY)=.*/\1=****/I' \
  >> "$REPORT_FILE" 2>&1 || true

log ""
log "STEP 7: Test runner once now"

set +e
"$FETCH52W_RUNNER" >> "$REPORT_FILE" 2>&1
TEST_EXIT=$?
set -e

log "TEST_RUN_EXIT=$TEST_EXIT"

log ""
log "STEP 8: Install cron for 4:30 PM weekdays"

CRON_LINE="30 16 * * 1-5 $FETCH52W_RUNNER >> $FETCH52W_LOG_DIR/cron_entry.log 2>&1"

TMP_CRON="$(mktemp)"
crontab -l 2>/dev/null | grep -v "$FETCH52W_RUNNER" > "$TMP_CRON" || true
echo "$CRON_LINE" >> "$TMP_CRON"
crontab "$TMP_CRON"
rm -f "$TMP_CRON"

log "CRON_INSTALLED=$CRON_LINE"

log ""
log "STEP 9: Final crontab"
crontab -l >> "$REPORT_FILE" 2>&1

log ""
log "STEP 10: Latest logs"
ls -lah "$FETCH52W_LOG_DIR" >> "$REPORT_FILE" 2>&1 || true
tail -n 160 "$FETCH52W_LOG_DIR"/fetch52w_*.log >> "$REPORT_FILE" 2>&1 || true
tail -n 160 "$FETCH52W_LOG_DIR"/fetch52w_run_*.log >> "$REPORT_FILE" 2>&1 || true
tail -n 160 "$FETCH52W_LOG_DIR"/fetch52w_email_*.log >> "$REPORT_FILE" 2>&1 || true
tail -n 160 "$FETCH52W_LOG_DIR"/fetch52w_container_*.log >> "$REPORT_FILE" 2>&1 || true
tail -n 160 "$FETCH52W_LOG_DIR"/atms_cli_fetch_52w_*.log >> "$REPORT_FILE" 2>&1 || true

log ""
log "=================================================="
log "SETUP COMPLETED"
log "=================================================="
log "ENDED: $(date '+%Y-%m-%d %H:%M:%S')"
log "REPORT_FILE=$REPORT_FILE"
