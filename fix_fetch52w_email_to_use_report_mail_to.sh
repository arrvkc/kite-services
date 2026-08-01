#!/bin/bash
set -euo pipefail

REPORT_FILE="/opt/kite_services/fix_fetch52w_email_to_use_report_mail_to_report.txt"
EMAIL_SCRIPT="/opt/kite_services/send_fetch52w_atms_email.py"
RUNNER="/opt/kite_services/run_fetch52w_atms_cron.sh"
LOG_DIR="/opt/kite_services/logs/fetch52w_atms"

{
  echo "=================================================="
  echo "FIX FETCH52W EMAIL TO USE REPORT_MAIL_TO REPORT"
  echo "=================================================="
  echo "STARTED: $(date '+%Y-%m-%d %H:%M:%S')"
  echo "HOST   : $(hostname)"
  echo ""
} > "$REPORT_FILE"

log() {
  echo "$1" | tee -a "$REPORT_FILE"
}

if [ ! -f "$EMAIL_SCRIPT" ]; then
  log "ERROR: $EMAIL_SCRIPT not found"
  exit 1
fi

if [ ! -x "$RUNNER" ]; then
  log "ERROR: $RUNNER not found or not executable"
  exit 1
fi

BACKUP_FILE="${EMAIL_SCRIPT}.bak_report_mail_to_$(date +%Y%m%d_%H%M%S)"
cp "$EMAIL_SCRIPT" "$BACKUP_FILE"

log "BACKUP_FILE=$BACKUP_FILE"

python3 - "$EMAIL_SCRIPT" <<'PY' >> "$REPORT_FILE" 2>&1
from pathlib import Path
import sys

p = Path(sys.argv[1])
text = p.read_text()

old = '''    recipients_raw = first_env(
        "FETCH52W_EMAIL_TO",
        "FETCH52W_ALERT_EMAIL_TO",
        "ALERT_EMAIL_TO",
        "EMAIL_TO",
        "REPORT_EMAIL_TO",
        "STRATEGY_EMAIL_TO",
        "MAIL_RECIPIENTS",
    )
'''

new = '''    recipients_raw = first_env(
        "FETCH52W_EMAIL_TO",
        "REPORT_MAIL_TO",
        "FETCH52W_ALERT_EMAIL_TO",
        "ALERT_EMAIL_TO",
        "EMAIL_TO",
        "REPORT_EMAIL_TO",
        "STRATEGY_EMAIL_TO",
        "MAIL_RECIPIENTS",
    )
'''

if old not in text:
    print("WARNING: exact recipient block not found; trying targeted insertion")
    if '"REPORT_MAIL_TO"' not in text:
        text = text.replace('"FETCH52W_EMAIL_TO",', '"FETCH52W_EMAIL_TO",\\n        "REPORT_MAIL_TO",', 1)
        print("PATCH_RESULT: inserted REPORT_MAIL_TO after FETCH52W_EMAIL_TO")
    else:
        print("PATCH_RESULT: REPORT_MAIL_TO already present")
else:
    text = text.replace(old, new, 1)
    print("PATCH_RESULT: replaced recipient block with REPORT_MAIL_TO support")

old_missing = '''        missing.append("FETCH52W_EMAIL_TO or ALERT_EMAIL_TO or REPORT_EMAIL_TO or STRATEGY_EMAIL_TO")'''
new_missing = '''        missing.append("FETCH52W_EMAIL_TO or REPORT_MAIL_TO or ALERT_EMAIL_TO or REPORT_EMAIL_TO or STRATEGY_EMAIL_TO")'''

text = text.replace(old_missing, new_missing)

p.write_text(text)
PY

log ""
log "STEP 1: Verify script now references REPORT_MAIL_TO"
grep -n "REPORT_MAIL_TO" "$EMAIL_SCRIPT" | tee -a "$REPORT_FILE"

log ""
log "STEP 2: Compile email script"
/opt/kite_services/venv/bin/python -m py_compile "$EMAIL_SCRIPT" >> "$REPORT_FILE" 2>&1
log "COMPILE_OK"

log ""
log "STEP 3: Test fetch52w runner again"
set +e
"$RUNNER" >> "$REPORT_FILE" 2>&1
TEST_EXIT=$?
set -e

log "TEST_RUN_EXIT=$TEST_EXIT"

log ""
log "STEP 4: Latest email log"
ls -1t "$LOG_DIR"/fetch52w_email_*.log 2>/dev/null | head -n 1 | while read f; do
  echo "LATEST_EMAIL_LOG=$f" | tee -a "$REPORT_FILE"
  tail -n 120 "$f" >> "$REPORT_FILE" 2>&1
done

log ""
log "STEP 5: Latest master log"
tail -n 120 "$LOG_DIR"/fetch52w_$(date +%F).log >> "$REPORT_FILE" 2>&1 || true

log ""
log "=================================================="
log "FIX COMPLETED"
log "=================================================="
log "ENDED: $(date '+%Y-%m-%d %H:%M:%S')"
log "REPORT_FILE=$REPORT_FILE"
