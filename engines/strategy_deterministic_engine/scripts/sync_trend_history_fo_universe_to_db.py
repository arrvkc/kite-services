from __future__ import annotations

import argparse
import hashlib
import json
import math
from datetime import date
from types import SimpleNamespace
from typing import Any
import uuid

import pandas as pd
from kiteconnect import KiteConnect
from kiteconnect.exceptions import TokenException

from services.kite_credentials_service import get_kite_credentials
from services.nse_final_close import NseFinalCloseProvider, NseFinalCloseUnavailable
from engines.strategy_deterministic_engine.adapters.trend_identifier_adapter import (
    TrendIdentifierKiteAdapter,
)
from engines.trend_identifier.trend_identifier.runners.equity_trend_history_runner import (
    EquityTrendHistoryRunner,
    NoTradingActivityCandidate,
    TargetDateHistoryUnavailableCandidate,
    TargetSessionNotFinalized,
    canonical_bars,
)
from engines.strategy_deterministic_engine.db.postgres import get_engine
from engines.strategy_deterministic_engine.db.upserts import (
    persist_strict_trend_preparation,
    persist_pending_historical_acquisition,
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


def target_date_unavailable_exclusion(
    candidate: TargetDateHistoryUnavailableCandidate,
) -> dict:
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
            "required_interval": evidence.required_interval,
            "target_daily_candle_count": evidence.target_daily_candle_count,
            "target_intraday_candle_count": evidence.target_intraday_candle_count,
            "latest_daily_timestamp": evidence.latest_daily_timestamp,
            "latest_intraday_timestamp": evidence.latest_intraday_timestamp,
            "market_session_confirmation": "PREPARED_PEER_TARGET_SESSION",
        },
    }


def preparation_exclusion(candidate) -> dict:
    if isinstance(candidate, NoTradingActivityCandidate):
        return no_trading_exclusion(candidate)
    if isinstance(candidate, TargetDateHistoryUnavailableCandidate):
        return target_date_unavailable_exclusion(candidate)
    raise RuntimeError("Unsupported governed Trend preparation exclusion.")


def exception_detail(exc: Exception) -> tuple[str, str]:
    exception_type = type(exc).__name__
    message = " ".join(str(exc).split()) or "No exception message supplied."
    return exception_type, message[:500]


def build_strict_preparation_manifest(
    requested_symbols: list[str],
    prepared_symbols: set[str],
    candidates: list[
        NoTradingActivityCandidate | TargetDateHistoryUnavailableCandidate
    ],
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
        if evidence.target_date != end_date:
            raise RuntimeError("No-trading evidence did not satisfy the strict contract.")
        if isinstance(candidate, NoTradingActivityCandidate) and (
            evidence.daily_volume != 0 or evidence.intraday_candle_count != 0
        ):
            raise RuntimeError("No-trading evidence did not satisfy the strict contract.")
        if isinstance(candidate, TargetDateHistoryUnavailableCandidate) and (
            evidence.target_daily_candle_count != 0
            or evidence.target_intraday_candle_count != 0
        ):
            raise RuntimeError(
                "Target-date-unavailable evidence did not satisfy the strict contract."
            )
    exclusions = [
        preparation_exclusion(candidate)
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


def dataset_identity_hash(*, result, interval: str, content_hash: str) -> str:
    payload = {
        "source": "ZERODHA_HISTORICAL_DATA",
        "symbol": result.symbol,
        "exchange": result.exchange,
        "tradingsymbol": result.tradingsymbol,
        "instrument_token": int(result.instrument_token),
        "interval": interval,
        "content_hash": content_hash,
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def build_window_evidence(acquisition_id: str, args, result) -> dict:
    """Serialize one coherent source window without publishing analytics."""
    window = result.source_window
    datasets = []
    snapshots = []
    bars = []
    daily_snapshot_id = None
    for interval, frame, content_hash in (
        ("day", window.daily, window.daily_content_hash),
        ("60minute", window.hourly, window.hourly_content_hash),
    ):
        snapshot_id = str(uuid.uuid4())
        if interval == "day":
            daily_snapshot_id = snapshot_id
        canonical = canonical_bars(frame)
        if not canonical:
            raise RuntimeError(f"{result.symbol} {interval} acquisition is empty.")
        dataset_id = dataset_identity_hash(
            result=result,
            interval=interval,
            content_hash=content_hash,
        )
        datasets.append(
            {
                "id": dataset_id,
                "source": "ZERODHA_HISTORICAL_DATA",
                "symbol": result.symbol,
                "exchange": result.exchange,
                "tradingsymbol": result.tradingsymbol,
                "instrument_token": result.instrument_token,
                "interval": interval,
                "covered_from": pd.Timestamp(frame["timestamp"].min()).to_pydatetime(),
                "covered_to": pd.Timestamp(frame["timestamp"].max()).to_pydatetime(),
                "bar_count": len(canonical),
                "content_hash": content_hash,
            }
        )
        snapshots.append(
            {
                "id": snapshot_id,
                "acquisition_id": acquisition_id,
                "dataset_id": dataset_id,
                "source_account": args.user_id,
                "symbol": result.symbol,
                "exchange": result.exchange,
                "tradingsymbol": result.tradingsymbol,
                "instrument_token": result.instrument_token,
                "interval": interval,
                "target_session_date": window.target_session_date,
                "covered_from": pd.Timestamp(frame["timestamp"].min()).to_pydatetime(),
                "covered_to": pd.Timestamp(frame["timestamp"].max()).to_pydatetime(),
                "retrieved_at": window.retrieved_at,
                "bar_count": len(canonical),
                "content_hash": content_hash,
                "validation_status": window.finality_status,
                "finality_source": (
                    window.finality_source if interval == "day" else None
                ),
                "finality_source_key": (
                    window.finality_source_key if interval == "day" else None
                ),
                "finality_source_hash": (
                    window.finality_source_hash if interval == "day" else None
                ),
                "finality_close": (
                    window.finality_close if interval == "day" else None
                ),
                "finality_retrieved_at": (
                    window.finality_retrieved_at if interval == "day" else None
                ),
            }
        )
        for item in canonical:
            observed_at = pd.Timestamp(item["timestamp"])
            bars.append(
                {
                    "dataset_id": dataset_id,
                    "observed_at": observed_at.to_pydatetime(),
                    "session_date": observed_at.tz_convert("Asia/Kolkata").date(),
                    "open": item["open"],
                    "high": item["high"],
                    "low": item["low"],
                    "close": item["close"],
                    "volume": item["volume"],
                    "oi": item["oi"],
                }
            )
    if daily_snapshot_id is None:
        raise RuntimeError("A daily historical snapshot is required.")
    return {
        "datasets": datasets,
        "snapshots": snapshots,
        "bars": bars,
        "daily_snapshot_id": daily_snapshot_id,
    }


def build_acquisition_evidence(acquisition_id: str, args, result) -> dict:
    """Serialize the coherent source window and its derived Trend rows."""
    evidence = build_window_evidence(acquisition_id, args, result)
    trend_outputs = []
    for row in build_symbol_rows(args, result):
        trend_outputs.append(
            {
                **row,
                "acquisition_id": acquisition_id,
                "daily_snapshot_id": evidence["daily_snapshot_id"],
            }
        )
    return {
        "datasets": evidence["datasets"],
        "snapshots": evidence["snapshots"],
        "bars": evidence["bars"],
        "trend_outputs": trend_outputs,
    }


def acquisition_content_hash(snapshots: list[dict]) -> str:
    members = [
        {
            "symbol": item["symbol"],
            "interval": item["interval"],
            "dataset_id": item["dataset_id"],
            "content_hash": item["content_hash"],
        }
        for item in sorted(
            snapshots, key=lambda item: (item["symbol"], item["interval"])
        )
    ]
    return hashlib.sha256(
        json.dumps(members, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


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
    final_close_resolver = None
    if end_date is not None:
        try:
            final_close_resolver = NseFinalCloseProvider().fetch(end_date).resolve
        except NseFinalCloseUnavailable as exc:
            unavailable_message = str(exc)

            def unavailable_final_close(_instrument, _target_date):
                raise NseFinalCloseUnavailable(unavailable_message)

            final_close_resolver = unavailable_final_close

    engine = None if args.dry_run else get_engine()
    total_rows = 0
    failures: list[dict] = []
    exclusion_candidates: list[
        NoTradingActivityCandidate | TargetDateHistoryUnavailableCandidate
    ] = []
    strict_rows: list[dict] = []
    prepared_symbols: set[str] = set()
    acquisition_id = str(uuid.uuid4())
    acquisition_datasets: list[dict] = []
    acquisition_snapshots: list[dict] = []
    acquisition_bars: list[dict] = []
    immutable_trend_outputs: list[dict] = []

    for index, symbol in enumerate(symbols, start=1):
        try:
            result = history_runner.build_history_for_symbol(
                symbol=symbol,
                history_days=args.history_days,
                end_date=end_date,
                final_close_resolver=final_close_resolver,
            )

            symbol_rows = build_symbol_rows(args, result)
            prepared_symbols.add(symbol)

            if args.strict and not args.dry_run:
                evidence = build_acquisition_evidence(acquisition_id, args, result)
                acquisition_datasets.extend(evidence["datasets"])
                acquisition_snapshots.extend(evidence["snapshots"])
                acquisition_bars.extend(evidence["bars"])
                immutable_trend_outputs.extend(evidence["trend_outputs"])

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

        except TargetSessionNotFinalized as exc:
            exception_type, message = exception_detail(exc)
            failures.append(
                {
                    "symbol": symbol,
                    "type": exception_type,
                    "message": message,
                }
            )
            if args.strict and not args.dry_run and exc.source_window is not None:
                pending_result = SimpleNamespace(
                    symbol=exc.source_window.symbol,
                    exchange=exc.source_window.exchange,
                    tradingsymbol=exc.source_window.tradingsymbol,
                    instrument_token=exc.source_window.instrument_token,
                    source_window=exc.source_window,
                )
                evidence = build_window_evidence(
                    acquisition_id, args, pending_result
                )
                acquisition_datasets.extend(evidence["datasets"])
                acquisition_snapshots.extend(evidence["snapshots"])
                acquisition_bars.extend(evidence["bars"])
            print(
                f"[{index}/{len(symbols)}] PENDING {symbol}: "
                f"{exception_type}: {message}",
                flush=True,
            )
        except NoTradingActivityCandidate as exc:
            if args.strict and end_date is not None:
                exclusion_candidates.append(exc)
                print(
                    f"[{index}/{len(symbols)}] EXCLUDED {symbol}: {exc.reason}",
                    flush=True,
                )
            else:
                exception_type, message = exception_detail(exc)
                failures.append(
                    {"symbol": symbol, "type": exception_type, "message": message}
                )
                print(
                    f"[{index}/{len(symbols)}] FAIL {symbol}: "
                    f"{exception_type}: {message}",
                    flush=True,
                )
        except TargetDateHistoryUnavailableCandidate as exc:
            if args.strict and end_date is not None:
                exclusion_candidates.append(exc)
                print(
                    f"[{index}/{len(symbols)}] EXCLUDED {symbol}: {exc.reason}",
                    flush=True,
                )
            else:
                exception_type, message = exception_detail(exc)
                failures.append(
                    {"symbol": symbol, "type": exception_type, "message": message}
                )
                print(
                    f"[{index}/{len(symbols)}] FAIL {symbol}: "
                    f"{exception_type}: {message}",
                    flush=True,
                )
        except TokenException:
            failures.append(
                {
                    "symbol": symbol,
                    "type": "TokenException",
                    "message": "Kite authentication failed.",
                }
            )
            print(
                f"[{index}/{len(symbols)}] FAIL {symbol}: TokenException",
                flush=True,
            )
            print("AUTHENTICATION_FAILURE aborting_universe_sync=YES", flush=True)
            raise SystemExit(1) from None
        except Exception as exc:
            exception_type, message = exception_detail(exc)
            failures.append(
                {"symbol": symbol, "type": exception_type, "message": message}
            )
            print(
                f"[{index}/{len(symbols)}] FAIL {symbol}: "
                f"{exception_type}: {message}",
                flush=True,
            )

    if failures:
        for failure in failures[:20]:
            print(
                f"FAILURE {failure['symbol']}: {failure['type']}: "
                f"{failure['message']}"
            )
        if args.strict:
            retryable_finality = all(
                failure["type"] == "TargetSessionNotFinalized"
                for failure in failures
            )
            if (
                retryable_finality
                and not args.dry_run
                and acquisition_snapshots
            ):
                persist_pending_historical_acquisition(
                    engine,
                    acquisition={
                        "id": acquisition_id,
                        "source_account": args.user_id,
                        "target_session_date": end_date,
                        "retrieved_at": max(
                            item["retrieved_at"] for item in acquisition_snapshots
                        ),
                        "status": "PENDING",
                        "finality_status": "PENDING_FINALITY",
                        "content_hash": acquisition_content_hash(
                            acquisition_snapshots
                        ),
                        "datasets": acquisition_datasets,
                        "snapshots": acquisition_snapshots,
                        "bars": acquisition_bars,
                    },
                    batch_size=args.batch_size,
                )
                print(
                    "FINALITY_PENDING acquisition_id=" + acquisition_id,
                    flush=True,
                )
                raise SystemExit(75)
            raise SystemExit(1)

    manifest = None
    if args.strict and end_date is not None:
        try:
            manifest = build_strict_preparation_manifest(
                symbols,
                prepared_symbols,
                exclusion_candidates,
                end_date=end_date,
            )
        except RuntimeError as exc:
            exception_type, message = exception_detail(exc)
            print(
                f"STRICT_PREPARATION_FAIL error={exception_type}: {message}"
            )
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
                acquisition={
                    "id": acquisition_id,
                    "source_account": args.user_id,
                    "target_session_date": manifest["run_date"],
                    "retrieved_at": max(
                        item["retrieved_at"] for item in acquisition_snapshots
                    ),
                    "status": "COMPLETE",
                    "finality_status": "FINALIZED",
                    "content_hash": acquisition_content_hash(
                        acquisition_snapshots
                    ),
                    "datasets": acquisition_datasets,
                    "snapshots": acquisition_snapshots,
                    "bars": acquisition_bars,
                    "trend_outputs": immutable_trend_outputs,
                },
            )
        print(f"UPSERTED trend_history_fo_universe rows={written}")
        print(
            "STRICT_PREPARATION "
            f"requested={len(symbols)} prepared={len(prepared_symbols)} "
            f"governed_exclusions={len(exclusion_candidates)}"
        )
        print(
            "NO_TRADING_SYMBOLS="
            + (
                ",".join(
                    sorted(
                        item.evidence.symbol
                        for item in exclusion_candidates
                        if isinstance(item, NoTradingActivityCandidate)
                    )
                )
                or "NONE"
            )
        )
        print(
            "TARGET_DATE_HISTORY_UNAVAILABLE_SYMBOLS="
            + (
                ",".join(
                    sorted(
                        item.evidence.symbol
                        for item in exclusion_candidates
                        if isinstance(item, TargetDateHistoryUnavailableCandidate)
                    )
                )
                or "NONE"
            )
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
