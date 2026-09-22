"""Classify one NSE CM date before Strategy finality orchestration."""

from __future__ import annotations

import argparse
from datetime import date

from services.nse_trading_session import (
    NseTradingSessionProvider,
    SessionReadiness,
)


def main() -> None:
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--run-date")
    group.add_argument("--previous-session-before")
    args = parser.parse_args()
    provider = NseTradingSessionProvider()
    if args.previous_session_before:
        evidence = provider.previous_session(date.fromisoformat(args.previous_session_before))
    else:
        evidence = provider.resolve(date.fromisoformat(args.run_date))
    values = {
        "NSE_SESSION_RESULT": evidence.readiness.value,
        "SESSION_CLASSIFICATION": evidence.classification.value,
        "SESSION_DATE": evidence.session_date.isoformat(),
        "SESSION_SEGMENT": evidence.segment,
        "SESSION_START_AT": (
            evidence.session_start.isoformat() if evidence.session_start else "-"
        ),
        "SESSION_END_AT": (
            evidence.session_end.isoformat() if evidence.session_end else "-"
        ),
        "SESSION_SOURCE": evidence.source,
        "SESSION_PROVENANCE": evidence.provenance,
        "SESSION_CHECKED_AT": evidence.checked_at.isoformat(),
        "SESSION_REASON_CODE": evidence.reason_code,
    }
    for key, value in values.items():
        print(f"{key}={value}", flush=True)
    if evidence.readiness == SessionReadiness.NO_SESSION:
        raise SystemExit(10)
    if evidence.readiness == SessionReadiness.NOT_COMPLETE:
        raise SystemExit(12)
    if evidence.readiness == SessionReadiness.UNRESOLVED:
        raise SystemExit(78)


if __name__ == "__main__":
    main()
