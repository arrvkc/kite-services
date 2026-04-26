"""
Database connection utilities for Stop Engine persistence.

This module connects kite_services to the ATMS/eajee PostgreSQL database,
where stop lifecycle, latest stop state, and stop event history are stored.
"""

import os
from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


DATABASE_URL = os.getenv("ATMS_DATABASE_URL")

if not DATABASE_URL:
    raise RuntimeError(
        "ATMS_DATABASE_URL is not set. "
        "Set it to the ATMS/eajee PostgreSQL connection string."
    )


engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=10,
)

SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine,
)


@contextmanager
def get_stop_db_session():
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
