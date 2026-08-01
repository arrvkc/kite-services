#!/usr/bin/env bash
set -euo pipefail

: "${ATMS_DATABASE_URL:?ATMS_DATABASE_URL must be loaded before configuring host database access}"

KITE_SERVICES_BASE_DIR="${KITE_SERVICES_BASE_DIR:-/opt/kite_services}"
ATMS_POSTGRES_CONTAINER="${ATMS_POSTGRES_CONTAINER:-postgres}"

CURRENT_DB_HOST="$(
  "$KITE_SERVICES_BASE_DIR/venv/bin/python" - "$ATMS_DATABASE_URL" <<'PY_HOST'
from urllib.parse import urlsplit
import sys

print(urlsplit(sys.argv[1]).hostname or "")
PY_HOST
)"

if [[ "$CURRENT_DB_HOST" != "$ATMS_POSTGRES_CONTAINER" ]]; then
  export ATMS_DATABASE_URL
  return 0 2>/dev/null || exit 0
fi

if ! command -v docker >/dev/null 2>&1; then
  echo "ERROR: docker is required to resolve the PostgreSQL container address" >&2
  return 1 2>/dev/null || exit 1
fi

POSTGRES_HOST_IP="$(
  docker inspect "$ATMS_POSTGRES_CONTAINER" |
    "$KITE_SERVICES_BASE_DIR/venv/bin/python" -c '
import json
import sys

payload = json.load(sys.stdin)
if not payload:
    raise SystemExit("PostgreSQL container inspection returned no data")

networks = payload[0].get("NetworkSettings", {}).get("Networks", {})
for network in networks.values():
    address = str(network.get("IPAddress") or "").strip()
    aliases = network.get("Aliases") or []
    if address and "postgres" in aliases:
        print(address)
        break
else:
    for network in networks.values():
        address = str(network.get("IPAddress") or "").strip()
        if address:
            print(address)
            break
    else:
        raise SystemExit("PostgreSQL container has no usable IPv4 address")
'
)"

if [[ -z "$POSTGRES_HOST_IP" ]]; then
  echo "ERROR: Could not resolve PostgreSQL container address" >&2
  return 1 2>/dev/null || exit 1
fi

ATMS_DATABASE_URL="$(
  "$KITE_SERVICES_BASE_DIR/venv/bin/python" - \
    "$ATMS_DATABASE_URL" "$POSTGRES_HOST_IP" <<'PY_URL'
from urllib.parse import urlsplit, urlunsplit
import sys

database_url = sys.argv[1]
database_host = sys.argv[2]

parts = urlsplit(database_url)
raw_userinfo = parts.netloc.rsplit("@", 1)[0] if "@" in parts.netloc else ""
port = parts.port or 5432

new_netloc = f"{database_host}:{port}"
if raw_userinfo:
    new_netloc = f"{raw_userinfo}@{new_netloc}"

print(
    urlunsplit(
        (
            parts.scheme,
            new_netloc,
            parts.path,
            parts.query,
            parts.fragment,
        )
    )
)
PY_URL
)"

export ATMS_DATABASE_URL
