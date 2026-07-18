#!/bin/bash
set +e

REPORT_FILE="/opt/kite_services/find_atms_compose_for_fetch52w_report.txt"

{
  echo "=================================================="
  echo "FIND ATMS COMPOSE FOR FETCH52W REPORT"
  echo "=================================================="
  echo "STARTED: $(date '+%Y-%m-%d %H:%M:%S')"
  echo "HOST   : $(hostname)"
  echo ""
} > "$REPORT_FILE"

log() {
  echo "$1" | tee -a "$REPORT_FILE"
}

run_cmd() {
  local title="$1"
  local cmd="$2"

  {
    echo ""
    echo "--------------------------------------------------"
    echo "$title"
    echo "--------------------------------------------------"
    echo "COMMAND: $cmd"
    echo "--------------------------------------------------"
    eval "$cmd"
    echo "EXIT_CODE: $?"
  } >> "$REPORT_FILE" 2>&1
}

log "STEP 1: Docker running containers"
run_cmd "docker ps" "docker ps"

log ""
log "STEP 2: Docker compose files under common roots"
run_cmd "find compose files" "find /opt /home /root -maxdepth 10 \\( -name 'docker-compose.yml' -o -name 'docker-compose.yaml' -o -name 'compose.yml' -o -name 'compose.yaml' \\) 2>/dev/null"

log ""
log "STEP 3: Direct search for cmd_atms.py on host"
run_cmd "find cmd_atms.py" "find /opt /home /root -maxdepth 12 -name cmd_atms.py 2>/dev/null"

log ""
log "STEP 4: Direct search for atms directories"
run_cmd "find atms dirs" "find /opt /home /root -maxdepth 8 -type d -iname '*atms*' 2>/dev/null"

log ""
log "STEP 5: Inspect compose candidates"

COMPOSE_FILES="$(find /opt /home /root -maxdepth 10 \( -name 'docker-compose.yml' -o -name 'docker-compose.yaml' -o -name 'compose.yml' -o -name 'compose.yaml' \) 2>/dev/null)"

for f in $COMPOSE_FILES; do
  d="$(dirname "$f")"
  {
    echo ""
    echo "=================================================="
    echo "COMPOSE CANDIDATE: $f"
    echo "DIR: $d"
    echo "=================================================="

    cd "$d" || continue

    echo ""
    echo "SERVICES:"
    docker-compose config --services 2>&1 || true

    echo ""
    echo "PS:"
    docker-compose ps 2>&1 || true

    echo ""
    echo "TRY website cmd_atms:"
    docker-compose exec -T website sh -lc 'pwd; find . -name cmd_atms.py -print; python ./cli/commands/cmd_atms.py --help 2>&1 | grep -i fetch || true' 2>&1 || true

    echo ""
    echo "TRY atms cmd_atms:"
    docker-compose exec -T atms sh -lc 'pwd; find . -name cmd_atms.py -print; python ./cli/commands/cmd_atms.py --help 2>&1 | grep -i fetch || true' 2>&1 || true

  } >> "$REPORT_FILE" 2>&1
done

log ""
log "=================================================="
log "DISCOVERY COMPLETED"
log "=================================================="
log "ENDED: $(date '+%Y-%m-%d %H:%M:%S')"
log "REPORT_FILE: $REPORT_FILE"

echo ""
echo "Upload/paste this report:"
echo "$REPORT_FILE"
