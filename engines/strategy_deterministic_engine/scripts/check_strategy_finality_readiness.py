"""Bounded preflight for official NSE/Zerodha target-session reconciliation."""

from __future__ import annotations

import argparse
from datetime import date, datetime
from zoneinfo import ZoneInfo

from kiteconnect import KiteConnect
from kiteconnect.exceptions import TokenException

from services.kite_credentials_service import get_kite_credentials
from services.nse_final_close import (
    NseFinalCloseProvider,
    NseFinalCloseUnavailable,
)
from engines.strategy_deterministic_engine.adapters.trend_identifier_adapter import (  # noqa: E501
    TrendIdentifierKiteAdapter,
)
from engines.trend_identifier.trend_identifier.runners.equity_trend_history_runner import (  # noqa: E501
    EquityTrendHistoryRunner,
    GOVERNED_EXPIRED_INDEX_SYMBOLS,
    TargetSessionNotFinalized,
)


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser()
    result.add_argument("user_id")
    result.add_argument("--run-date", required=True)
    return result


def main() -> None:
    args = parser().parse_args()
    run_date = date.fromisoformat(args.run_date)
    api_key, access_token = get_kite_credentials(args.user_id)
    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    symbols = TrendIdentifierKiteAdapter(kite=kite).get_fo_universe_symbols()
    try:
        catalog = NseFinalCloseProvider().fetch(run_date)
        symbols = catalog.filter_current_universe(symbols)
    except NseFinalCloseUnavailable as exc:
        print(
            "FINALITY_READINESS=PENDING reason_code=NSE_FINAL_UNAVAILABLE "
            f"run_date={run_date} account={args.user_id} detail={exc}",
            flush=True,
        )
        raise SystemExit(75) from None

    runner = EquityTrendHistoryRunner(kite=kite, exchange="NSE")
    pending: list[str] = []
    try:
        for symbol in symbols:
            contract = catalog.nearest_index_future(symbol)
            if contract is not None and contract[0] < datetime.now(ZoneInfo("Asia/Kolkata")).date() and symbol in GOVERNED_EXPIRED_INDEX_SYMBOLS:
                print(f"READINESS_DEFERRED_TO_COHERENT_ACQUISITION symbol={symbol} reason=expired_index_contract", flush=True)
                continue
            try:
                runner.check_target_session_finality(
                    symbol, run_date, catalog.resolve
                )
            except TargetSessionNotFinalized:
                pending.append(symbol)
    except TokenException:
        print(
            "FINALITY_READINESS=FAIL reason_code=KITE_AUTHENTICATION",
            flush=True,
        )
        raise SystemExit(1) from None

    if pending:
        print(
            "FINALITY_READINESS=PENDING reason_code=ZERODHA_NSE_MISMATCH "
            f"run_date={run_date} account={args.user_id} "
            f"pending_count={len(pending)} "
            f"pending_symbols={','.join(sorted(pending))}",
            flush=True,
        )
        raise SystemExit(75)
    print(
        "FINALITY_READINESS=READY reason_code=NSE_ZERODHA_RECONCILED "
        f"run_date={run_date} account={args.user_id} symbols={len(symbols)}",
        flush=True,
    )


if __name__ == "__main__":
    main()
