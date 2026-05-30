import json
import os
import re
import subprocess
from pathlib import Path


BASE_DIR = Path(os.getenv("KITE_SERVICES_BASE_DIR", Path.home() / "kite_services"))

CREDENTIAL_MODE = os.getenv("EAJEE_CREDENTIAL_MODE", "auto")
# auto | local_json | server_db


def validate_user_id(user_id):
    if not re.match(r"^[A-Z0-9_@.-]+$", user_id):
        raise ValueError(f"Unsafe user_id: {user_id}")


def get_credentials_from_local_json(user_id):
    path = BASE_DIR / "secrets" / f"zerodha_{user_id}.json"

    if not path.exists():
        raise RuntimeError(f"Credential file not found: {path}")

    data = json.loads(path.read_text())

    return {
        "username": data.get("username") or user_id,
        "zerodha_user_id": data.get("zerodha_user_id") or user_id,
        "zerodha_password": data.get("zerodha_password"),
        "zerodha_totp_hash": data.get("zerodha_totp_hash"),
    }


def get_credentials_from_server_db(user_id):
    validate_user_id(user_id)

    query = f"""
SELECT
    username,
    COALESCE(zerodha_user_id, ''),
    COALESCE(zerodha_password, ''),
    COALESCE(zerodha_totp_hash, '')
FROM users
WHERE username = '{user_id}'
   OR zerodha_user_id = '{user_id}'
LIMIT 1;
"""

    result = subprocess.run(
        [
            "docker",
            "exec",
            "postgres",
            "psql",
            "-U",
            "postgres",
            "-d",
            "atms",
            "-t",
            "-A",
            "-F",
            "|",
            "-c",
            query,
        ],
        text=True,
        capture_output=True,
        check=False,
    )

    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip())

    raw = result.stdout.strip()

    if not raw:
        raise RuntimeError(f"No credential row found for {user_id}")

    parts = raw.split("|")

    if len(parts) != 4:
        raise RuntimeError(f"Unexpected credential format for {user_id}: {raw}")

    username, zerodha_user_id, zerodha_password, zerodha_totp_hash = parts

    return {
        "username": username,
        "zerodha_user_id": zerodha_user_id or username,
        "zerodha_password": zerodha_password,
        "zerodha_totp_hash": zerodha_totp_hash,
    }


def get_zerodha_credentials(user_id):
    validate_user_id(user_id)

    if CREDENTIAL_MODE == "local_json":
        creds = get_credentials_from_local_json(user_id)
    elif CREDENTIAL_MODE == "server_db":
        creds = get_credentials_from_server_db(user_id)
    else:
        local_file = BASE_DIR / "secrets" / f"zerodha_{user_id}.json"
        if local_file.exists():
            creds = get_credentials_from_local_json(user_id)
        else:
            creds = get_credentials_from_server_db(user_id)

    missing = [
        key
        for key in ["zerodha_user_id", "zerodha_password", "zerodha_totp_hash"]
        if not creds.get(key)
    ]

    if missing:
        raise RuntimeError(f"Missing credentials for {user_id}: {missing}")

    return creds
