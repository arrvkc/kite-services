from __future__ import annotations

import argparse
import math
from datetime import date
from typing import Any

import pandas as pd
from kiteconnect import KiteConnect
from kiteconnect.exceptions import TokenException

from services.kite_credentials_service import get_kite_credentials
from engines.strategy_deterministic_engine.adapters.trend_identifier_adapter import (
    TrendIdentifierKiteAdapter,
)
from engines.trend_identifier.trend_identifier.runners.equity_trend_history_runner import (
    EquityTrendHistoryRunner,
    NoTradingActivityCandidate,
)
from engines.strategy_deterministic_engine.db.postgres import get_engine
from engines.strategy_deterministic_engine.db.upserts import (
    persist_strict_trend_preparation,
    upsert_trend_history_rows,
)


def clean_value(value):
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if pd.isna(value):
        return None
    return value


def to_date(value) -> date:
    return pd.Timestamp(value).date()


def json_value(value: Any) -> Any:
    value = clean_value(value)
    if value is None:
        return None
    if hasattr(value, "item"):
        value = value.item()
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    return value


def no_trading_exclusion(candidate: NoTradingActivityCandidate) -> dict:
    evidence = candidate.evidence
    return {
        "symbol": evidence.symbol,
        "reason": candidate.reason,
        "stage": "TREND_PREPARATION",
        "target_date": evidence.target_date.isoformat(),
        "selected_instrument": {
            "exchange": evidence.exchange,
            "tradingsymbol": evidence.tradingsymbol,
            "instrument_token": evidence.instrument_token,
        },
        "evidence": {
            "daily_timestamp": evidence.daily_timestamp,
            "daily_open": json_value(evidence.daily_open),
            "daily_high": json_value(evidence.daily_high),
            "daily_low": json_value(evidence.daily_low),
            "daily_close": json_value(evidence.daily_close),
            "daily_volume": json_value(evidence.daily_volume),
            "daily_oi": json_value(evidence.daily_oi),
            "required_interval": evidence.required_interval,
            "target_intraday_candle_count": evidence.intraday_candle_count,
            "market_session_confirmation": "PREPARED_PEER_TARGET_SESSION",
        },
    }


def build_strict_preparation_manifest(
    requested_symbols: list[str],
    prepared_symbols: set[str],
    candidates: list[NoTradingActivityCandidate],
    *,
    end_date: date,
) -> dict:
    requested = [symbol.strip().upper() for symbol in requested_symbols]
    if len(requested) != len(set(requested)):
        raise RuntimeError("Requested Strategy universe contains duplicate symbols.")
    candidate_symbols = {candidate.evidence.symbol for candidate in candidates}
    if prepared_symbols & candidate_symbols:
        raise RuntimeError("A no-trading symbol was also prepared as a target session.")
    if set(requested) != prepared_symbols | candidate_symbols:
        raise RuntimeError("Strict Trend preparation did not explain every requested symbol.")
    if candidates and not prepared_symbols:
        raise RuntimeError(
            "No prepared peer established that the target exchange session existed."
        )
    for candidate in candidates:
        evidence = candidate.evidence
        if (
            evidence.target_date != end_date
            or evidence.daily_volume != 0
            or evidence.intraday_candle_count != 0
        ):
            raise RuntimeError("No-trading evidence did not satisfy the strict contract.")
    exclusions = [
        no_trading_exclusion(candidate)
        for candidate in sorted(candidates, key=lambda item: item.evidence.symbol)
    ]
    return {
        "run_date": end_date,
        "requested_symbols_count": len(requested),
        "prepared_symbols_count": len(prepared_symbols),
        "exclusions": exclusions,
    }


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Sync Trend Identifier history for all F&O symbols into ATMS PostgreSQL."
    )
    parser.add_argument("user_id", help="Zerodha user id used to fetch Kite credentials")
    parser.add_argument("--history-days", type=int, default=5)
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--symbols", help="Optional comma-separated symbols for testing")
    parser.add_argument(
        "--end-date",
        help="Require history to end on this completed market session (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Return non-zero if any requested symbol cannot be reconstructed.",
    )
    parser.add_argument(
        "--restart-unverified-completed-run",
        action="store_true",
        help=(
            "Allow governed recovery to replace a completed run only when its "
            "exact-input provenance is entirely absent."
        ),
    )
    parser.add_argument("--dry-run", action="store_true")
    return parser


def build_symbol_rows(args, result) -> list[dict]:
    history_df = result.history.copy()
    symbol_rows: list[dict] = []

    for _, row in history_df.iterrows():
        symbol_rows.append(
            {
                "user_id": args.user_id,
                "symbol": result.symbol,
                "trade_date": to_date(row["date"]),
                "close": clean_value(row.get("close")),
                "label": clean_value(row.get("label")),
                "confidence": clean_value(row.get("confidence")),
                "aggregate_score": clean_value(row.get("aggregate_score")),
                "internal_state": clean_value(row.get("internal_state")),
                "exchange": result.exchange,
                "tradingsymbol": result.tradingsymbol,
                "instrument_token": result.instrument_token,
            }
        )

    return symbol_rows


def main() -> None:
    args = build_argument_parser().parse_args()
    end_date = date.fromisoformat(args.end_date) if args.end_date else None

    api_key, access_token = get_kite_credentials(args.user_id)
    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)

    adapter = TrendIdentifierKiteAdapter(kite=kite)
    symbols = (
        [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
        if args.symbols
        else adapter.get_fo_universe_symbols()
    )

    history_runner = EquityTrendHistoryRunner(kite=kite, exchange="NSE")

    engine = None if args.dry_run else get_engine()
    total_rows = 0
    failures: list[dict] = []
    no_trading_candidates: list[NoTradingActivityCandidate] = []
    strict_rows: list[dict] = []
    prepared_symbols: set[str] = set()

    for index, symbol in enumerate(symbols, start=1):
        try:
            result = history_runner.build_history_for_symbol(
                symbol=symbol,
                history_days=args.history_days,
                end_date=end_date,
            )

            symbol_rows = build_symbol_rows(args, result)
            prepared_symbols.add(symbol)

            if args.dry_run or args.strict:
                total_rows += len(symbol_rows)
                if args.strict and not args.dry_run:
                    strict_rows.extend(symbol_rows)
                    print(
                        f"[{index}/{len(symbols)}] PREPARED {symbol} rows={len(symbol_rows)}",
                        flush=True,
                    )
                else:
                    print(
                        f"[{index}/{len(symbols)}] DRY OK {symbol} rows={len(symbol_rows)}",
                        flush=True,
                    )
            else:
                written = upsert_trend_history_rows(
                    engine,
                    symbol_rows,
                    batch_size=args.batch_size,
                )
                total_rows += written
                print(f"[{index}/{len(symbols)}] OK {symbol} committed_rows={written}", flush=True)

        except NoTradingActivityCandidate as exc:
            if args.strict and end_date is not None:
                no_trading_candidates.append(exc)
                print(
                    f"[{index}/{len(symbols)}] EXCLUDED {symbol}: {exc.reason}",
                    flush=True,
                )
            else:
                failures.append({"symbol": symbol, "error": type(exc).__name__})
                print(
                    f"[{index}/{len(symbols)}] FAIL {symbol}: {type(exc).__name__}",
                    flush=True,
                )
        except TokenException:
            failures.append({"symbol": symbol, "error": "TokenException"})
            print(
                f"[{index}/{len(symbols)}] FAIL {symbol}: TokenException",
                flush=True,
            )
            print("AUTHENTICATION_FAILURE aborting_universe_sync=YES", flush=True)
            raise SystemExit(1) from None
        except Exception as exc:
            safe_error = type(exc).__name__
            failures.append({"symbol": symbol, "error": safe_error})
            print(f"[{index}/{len(symbols)}] FAIL {symbol}: {safe_error}", flush=True)

    if failures:
        for failure in failures[:20]:
            print(f"FAILURE {failure['symbol']}: {failure['error']}")
        if args.strict:
            raise SystemExit(1)

    manifest = None
    if args.strict and end_date is not None:
        try:
            manifest = build_strict_preparation_manifest(
                symbols,
                prepared_symbols,
                no_trading_candidates,
                end_date=end_date,
            )
        except RuntimeError as exc:
            print(f"STRICT_PREPARATION_FAIL error={type(exc).__name__}")
            raise SystemExit(1) from None

    if args.dry_run:
        print(f"DRY RUN: prepared_rows={total_rows} failures={len(failures)}")
    elif args.strict:
        if manifest is None:
            written = upsert_trend_history_rows(
                engine,
                strict_rows,
                batch_size=args.batch_size,
            )
        else:
            written = persist_strict_trend_preparation(
                engine,
                strict_rows,
                run_date=manifest["run_date"],
                generated_by_user_id=args.user_id,
                requested_symbols_count=manifest["requested_symbols_count"],
                prepared_symbols_count=manifest["prepared_symbols_count"],
                exclusions=manifest["exclusions"],
                batch_size=args.batch_size,
                allow_unverified_completed_restart=(
                    args.restart_unverified_completed_run
                ),
            )
        print(f"UPSERTED trend_history_fo_universe rows={written}")
        print(
            "STRICT_PREPARATION "
            f"requested={len(symbols)} prepared={len(prepared_symbols)} "
            f"no_trading_activity={len(no_trading_candidates)}"
        )
        print(
            "NO_TRADING_SYMBOLS="
            + (",".join(sorted(item.evidence.symbol for item in no_trading_candidates)) or "NONE")
        )
        print("FAILURES count=0")
    else:
        print(f"UPSERTED trend_history_fo_universe rows={total_rows}")
        print(f"FAILURES count={len(failures)}")
        if total_rows == 0:
            print("USABLE_TARGET_DATA=NO")
            raise SystemExit(1)


if __name__ == "__main__":
    main()
