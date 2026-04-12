"""Machine-readable deterministic logging."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict

from .exceptions import LogWriteError


def log_decision(instrument: str, asof_time: str, payload: Dict, log_path: str | None = None) -> None:
    # SPEC TRACE: Section 14 - logging guarantee
    if log_path is None:
        return
    path = Path(log_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    record = {"instrument": instrument, "asof_time": asof_time, "payload": payload}
    try:
        with path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(record, sort_keys=True) + "\n")
    except OSError as exc:
        raise LogWriteError(str(exc)) from exc
