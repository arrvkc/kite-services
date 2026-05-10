from __future__ import annotations

import os
from pathlib import Path
from sqlalchemy import create_engine


def load_env_file(path: str = ".env") -> None:
    env_path = Path(path)
    if not env_path.exists():
        return

    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


def get_atms_database_url() -> str:
    load_env_file()
    database_url = os.environ.get("ATMS_DATABASE_URL")
    if not database_url:
        raise RuntimeError("ATMS_DATABASE_URL is missing. Add it to .env or export it.")
    return database_url


def get_engine(database_url: str | None = None):
    return create_engine(
        database_url or get_atms_database_url(),
        pool_pre_ping=True,
        pool_recycle=1800,
    )
