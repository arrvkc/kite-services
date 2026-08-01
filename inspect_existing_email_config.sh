#!/bin/bash
set +e

REPORT_FILE="/opt/kite_services/inspect_existing_email_config_report.txt"
APP_DIR="/opt/kite_services"
EMAIL_SCRIPT="$APP_DIR/engines/strategy_deterministic_engine/reports/email_strategy_report.py"

{
  echo "=================================================="
  echo "INSPECT EXISTING EMAIL CONFIG REPORT"
  echo "=================================================="
  echo "STARTED: $(date '+%Y-%m-%d %H:%M:%S')"
  echo "HOST   : $(hostname)"
  echo ""
} > "$REPORT_FILE"

log() {
  echo "$1" | tee -a "$REPORT_FILE"
}

log "STEP 1: Check existing strategy email script"
if [ -f "$EMAIL_SCRIPT" ]; then
  echo "FOUND: $EMAIL_SCRIPT" >> "$REPORT_FILE"
  echo "" >> "$REPORT_FILE"

  echo "---- grep email-related code from email_strategy_report.py ----" >> "$REPORT_FILE"
  grep -nEi 'to|recipient|receiver|email|mail|smtp|send|subject|from|cc|bcc|environ|getenv|os\.environ' "$EMAIL_SCRIPT" \
    >> "$REPORT_FILE" 2>&1 || true

  echo "" >> "$REPORT_FILE"
  echo "---- first 260 lines of email_strategy_report.py ----" >> "$REPORT_FILE"
  nl -ba "$EMAIL_SCRIPT" | sed -n '1,260p' >> "$REPORT_FILE" 2>&1
else
  echo "NOT FOUND: $EMAIL_SCRIPT" >> "$REPORT_FILE"
fi

log "STEP 2: Check .env email keys without secrets"

echo "" >> "$REPORT_FILE"
echo "---- .env email-related keys masked ----" >> "$REPORT_FILE"

grep -nEi 'mail|smtp|email|recipient|receiver|to=|from=|sender|report' "$APP_DIR/.env" \
  | sed -E 's/(PASSWORD|SECRET|TOKEN|KEY|PASS|PWD)=.*/\1=****/I' \
  >> "$REPORT_FILE" 2>&1 || true

log "STEP 3: Check latest working email log"

echo "" >> "$REPORT_FILE"
echo "---- latest strategy email logs ----" >> "$REPORT_FILE"

ls -lah "$APP_DIR/logs/strategy_db_pipeline"/strategy_report_email_*.log >> "$REPORT_FILE" 2>&1 || true

echo "" >> "$REPORT_FILE"
echo "---- tail latest 3 strategy email logs ----" >> "$REPORT_FILE"

ls -1t "$APP_DIR/logs/strategy_db_pipeline"/strategy_report_email_*.log 2>/dev/null | head -n 3 | while read f; do
  echo "" >> "$REPORT_FILE"
  echo "===== $f =====" >> "$REPORT_FILE"
  tail -n 120 "$f" >> "$REPORT_FILE" 2>&1
done

log ""
log "=================================================="
log "INSPECTION COMPLETED"
log "=================================================="
log "REPORT_FILE=$REPORT_FILE"

echo ""
echo "Paste/upload this report:"
echo "$REPORT_FILE"
