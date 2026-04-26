import os
import subprocess
import sys
from typing import Tuple

DEFAULT_HOST = "root@eajee.in"
DEFAULT_DB_CONTAINER = "postgres"
DEFAULT_DB_USER = "postgres"
DEFAULT_DB_NAME = "atms"


def _run_local(cmd: str) -> str:
    out = subprocess.check_output(cmd, shell=True, text=True)
    return out.strip()


def _run_remote(host: str, cmd: str) -> str:
    out = subprocess.check_output(["ssh", host, cmd], text=True)
    return out.strip()


def get_kite_credentials(
    user_id: str,
    host: str = DEFAULT_HOST,
    db_container: str = DEFAULT_DB_CONTAINER,
    db_user: str = DEFAULT_DB_USER,
    db_name: str = DEFAULT_DB_NAME,
) -> Tuple[str, str]:

    sql = (
        "SELECT kite_api_key, kite_access_token "
        "FROM users "
        f"WHERE zerodha_user_id = '{user_id}' "
        "LIMIT 1;"
    )

    cmd = (
        f'docker exec -i {db_container} psql -U {db_user} -d {db_name} '
        f'-t -A -F "|" -c "{sql}"'
    )

    mode = os.getenv("KITE_CREDENTIALS_MODE", "remote").lower()

    if mode == "local":
        result = _run_local(cmd)
    else:
        result = _run_remote(host, cmd)

    if not result:
        raise Exception(f"No Kite credentials found for user_id={user_id}")

    try:
        api_key, access_token = result.split("|", 1)
    except ValueError:
        raise Exception(f"Invalid credential format for user_id={user_id}: {result}")

    if not api_key or not access_token:
        raise Exception(f"Incomplete credentials for user_id={user_id}")

    return api_key.strip(), access_token.strip()