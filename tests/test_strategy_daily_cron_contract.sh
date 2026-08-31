#!/usr/bin/env bash
set -euo pipefail

readonly REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
readonly PIPELINE="$REPO_ROOT/run_strategy_db_daily_cron.sh"
readonly ROOT="$(mktemp -d "${TMPDIR:-/tmp}/strategy-daily-contract.XXXXXX")"
trap 'rm -rf "$ROOT"' EXIT

fail() { echo "FAIL: $*" >&2; exit 1; }

prepare() {
    rm -rf "$ROOT/app"
    mkdir -p "$ROOT/app/scripts/runtime"
    : > "$ROOT/app/.env"
    : > "$ROOT/app/scripts/runtime/configure_host_database_runtime.sh"
    : > "$ROOT/calls"
    cat > "$ROOT/python" <<'SH'
#!/usr/bin/env bash
printf '%s\n' "$*" >> "$EAJEE_DAILY_TEST_CALLS"
case "$*" in
  *validate_strategy_kite_credentials*) [[ "${FAIL_STAGE:-}" = credential ]] && exit 20 ;;
  *sync_trend_history*) [[ "${FAIL_STAGE:-}" = trend ]] && exit 21 ;;
  *sync_contract_snapshot*) [[ "${FAIL_STAGE:-}" = contract ]] && exit 22 ;;
  *verify_strategy_backfill_inputs*) [[ "${FAIL_STAGE:-}" = exact_input ]] && exit 23 ;;
  *run_strategy_engine_batch_from_db*) [[ "${FAIL_STAGE:-}" = strategy ]] && exit 24 ;;
esac
exit 0
SH
    chmod 0755 "$ROOT/python"
}

run_pipeline() {
    EAJEE_KITE_SERVICES_DIR="$ROOT/app" \
    EAJEE_KITE_PYTHON="$ROOT/python" \
    EAJEE_DAILY_TEST_CALLS="$ROOT/calls" \
    EAJEE_STRATEGY_RUN_DATE=2026-08-31 \
        "$PIPELINE"
}

assert_stops_after() {
    local stage="$1"
    local expected_calls="$2"
    prepare
    set +e
    FAIL_STAGE="$stage" run_pipeline >/dev/null 2>&1
    local rc=$?
    set -e
    [[ "$rc" -ne 0 ]] || fail "$stage failure returned success"
    [[ "$(wc -l < "$ROOT/calls" | tr -d ' ')" = "$expected_calls" ]] || \
        fail "$stage failure did not stop downstream execution"
}

assert_stops_after credential 1
echo "PASS invalid credential stops before Trend"
assert_stops_after trend 2
echo "PASS Trend failure stops Contract and Strategy"
assert_stops_after contract 3
echo "PASS Contract failure stops exact gate and Strategy"
assert_stops_after exact_input 4
echo "PASS exact-input failure stops Strategy"
assert_stops_after strategy 5
echo "PASS Strategy failure propagates"

prepare
run_pipeline >/dev/null
[[ "$(wc -l < "$ROOT/calls" | tr -d ' ')" = 5 ]] || fail "successful stage count changed"
grep -Fq 'validate_strategy_kite_credentials.py OMK569' "$ROOT/calls"
grep -Fq 'sync_trend_history_fo_universe_to_db.py OMK569 --history-days 5 --end-date 2026-08-31 --strict' "$ROOT/calls"
grep -Fq 'sync_contract_snapshot_fo_universe_to_db.py OMK569 --selection-date 2026-08-31 --strict' "$ROOT/calls"
grep -Fq 'verify_strategy_backfill_inputs.py --run-date 2026-08-31 --history-days 5' "$ROOT/calls"
grep -Fq 'run_strategy_engine_batch_from_db.py OMK569 --run-date 2026-08-31 --history-days 5 --require-exact-contract-snapshot' "$ROOT/calls"
if grep -Fq 'email_strategy_report.py' "$PIPELINE"; then
    fail "daily Kite pipeline still owns success email"
fi
echo "PASS daily pipeline uses exact-date fail-closed contract and does not email"
