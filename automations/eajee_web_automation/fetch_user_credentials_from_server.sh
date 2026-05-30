#!/usr/bin/env bash
set -euo pipefail

USER_ID="${1:-XJ1877}"

mkdir -p secrets

ssh root@eajee.in "docker exec postgres psql -U postgres -d atms -t -A -F '|' -c \"
SELECT
    username,
    COALESCE(zerodha_user_id, ''),
    COALESCE(zerodha_password, ''),
    COALESCE(zerodha_totp_hash, '')
FROM users
WHERE username = '${USER_ID}'
   OR zerodha_user_id = '${USER_ID}'
LIMIT 1;
\"" > "/tmp/${USER_ID}_credential_raw.txt"

python - <<PY
import json
from pathlib import Path

user_id = "${USER_ID}"
raw_path = Path(f"/tmp/{user_id}_credential_raw.txt")
out_path = Path("secrets") / f"zerodha_{user_id}.json"

raw = raw_path.read_text().strip()

if not raw:
    raise SystemExit(f"No credential row found for {user_id}")

parts = raw.split("|")

if len(parts) != 4:
    raise SystemExit(f"Unexpected credential format: {raw}")

username, zerodha_user_id, zerodha_password, zerodha_totp_hash = parts

data = {
    "username": username,
    "zerodha_user_id": zerodha_user_id or username,
    "zerodha_password": zerodha_password,
    "zerodha_totp_hash": zerodha_totp_hash,
}

out_path.write_text(json.dumps(data, indent=2))

print(f"Saved credentials to {out_path}")
print("username:", username)
print("zerodha_user_id:", zerodha_user_id or username)
print("zerodha_password:", "PRESENT" if zerodha_password else "MISSING")
print("zerodha_totp_hash:", "PRESENT" if zerodha_totp_hash else "MISSING")
PY

rm -f "/tmp/${USER_ID}_credential_raw.txt"
