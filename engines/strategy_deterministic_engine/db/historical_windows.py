"""Read-only recovery of a complete, previously finalized source window."""
from __future__ import annotations

import hashlib
import json
from datetime import date

import pandas as pd
from sqlalchemy import text

from engines.strategy_deterministic_engine.db.upserts import validate_historical_chunk_manifests
from engines.trend_identifier.trend_identifier.runners.equity_trend_history_runner import (
    CoherentHistoricalWindow, bars_content_hash, canonical_bars,
)


def load_finalized_source_window(engine, account: str, symbol: str, target: date, contract: str):
    """Pending universe acquisitions may contain finalized per-symbol windows.

    Both intervals must belong to the same acquisition/account/date/contract.
    Validate the original manifests and every chunk before returning any bars.
    No source row, chunk or published recommendation is modified.
    """
    with engine.connect() as connection:
        if connection.dialect.name == "postgresql":
            connection.execute(text("SET TRANSACTION READ ONLY"))
        daily = connection.execute(text("""
            SELECT d.*, h.id AS hourly_snapshot_id
            FROM historical_price_snapshots d JOIN historical_price_snapshots h
              ON h.acquisition_id=d.acquisition_id AND h.source_account=d.source_account
             AND h.symbol=d.symbol AND h.exchange=d.exchange
             AND h.tradingsymbol=d.tradingsymbol AND h.instrument_token=d.instrument_token
             AND h.target_session_date=d.target_session_date AND h.retrieved_at=d.retrieved_at
             AND h.interval='60minute' AND h.validation_status='FINALIZED'
            WHERE d.source_account=:account AND d.symbol=:symbol
              AND d.target_session_date=:target AND d.tradingsymbol=:contract
              AND d.exchange='NFO' AND d.interval='day' AND d.validation_status='FINALIZED'
              AND d.finality_source='NSE_FO_FUT_FINAL'
            ORDER BY d.retrieved_at DESC, d.id DESC LIMIT 1
        """), {"account": account, "symbol": symbol, "target": target, "contract": contract}).mappings().first()
        if daily is None:
            return None
        hourly = connection.execute(text("SELECT * FROM historical_price_snapshots WHERE id=:id"),
                                    {"id": daily["hourly_snapshot_id"]}).mappings().one()
        snapshots = [dict(daily), dict(hourly)]
        if any(row["source_account"] != account or row["symbol"] != symbol
               or row["exchange"] != "NFO" or row["tradingsymbol"] != contract
               or pd.Timestamp(row["target_session_date"]).date() != target
               or row["acquisition_id"] != daily["acquisition_id"]
               or row["retrieved_at"] != daily["retrieved_at"] for row in snapshots):
            raise RuntimeError("Archived source intervals have inconsistent identity.")
        all_chunks, all_references, frames = [], [], {}
        for snapshot in snapshots:
            chunks = connection.execute(text("""
                SELECT c.*, r.ordinal, r.snapshot_id, r.chunk_key AS reference_key
                FROM historical_price_snapshot_chunks r
                JOIN historical_price_chunks c ON c.id=r.chunk_id
                WHERE r.snapshot_id=:id ORDER BY r.ordinal
            """), {"id": snapshot["id"]}).mappings().all()
            parts = []
            for chunk in chunks:
                rows = connection.execute(text("""
                    SELECT observed_at AS timestamp, open, high, low, close, volume, oi
                    FROM historical_price_bars WHERE chunk_id=:id ORDER BY observed_at
                """), {"id": chunk["id"]}).mappings().all()
                if not rows:
                    raise RuntimeError("Archived historical chunk is empty.")
                frame = pd.DataFrame([dict(row) for row in rows])
                frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
                for column in ["open", "high", "low", "close", "volume", "oi"]:
                    frame[column] = pd.to_numeric(frame[column])
                if frame["oi"].isna().all():
                    frame = frame.drop(columns="oi")
                canonical = canonical_bars(frame)
                content_hash = hashlib.sha256(json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
                if len(frame) != chunk["bar_count"] or content_hash != chunk["content_hash"]:
                    raise RuntimeError("Archived historical chunk content does not match its evidence.")
                all_chunks.append(dict(chunk))
                all_references.append({"snapshot_id": snapshot["id"], "chunk_id": chunk["id"],
                                       "ordinal": chunk["ordinal"], "chunk_key": chunk["reference_key"]})
                parts.append(frame)
            if not parts:
                raise RuntimeError("Archived historical snapshot has no chunks.")
            frame = pd.concat(parts, ignore_index=True)
            if len(frame) != snapshot["bar_count"] or not frame["timestamp"].is_unique or not frame["timestamp"].is_monotonic_increasing:
                raise RuntimeError("Archived historical snapshot coverage is inconsistent.")
            if frame["timestamp"].dt.tz_convert("Asia/Kolkata").dt.date.max() > target:
                raise RuntimeError("Archived source contains future-session observations.")
            frames[snapshot["interval"]] = frame
        validate_historical_chunk_manifests({"snapshots": snapshots, "chunks": all_chunks,
                                             "snapshot_chunks": all_references})
        retrieved_at = pd.Timestamp(daily["retrieved_at"]).to_pydatetime()
        if retrieved_at.tzinfo is None:
            raise RuntimeError("Archived source retrieval time has no timezone.")
        return CoherentHistoricalWindow(
            symbol=symbol, exchange="NFO", tradingsymbol=contract,
            instrument_token=int(daily["instrument_token"]), target_session_date=target,
            retrieved_at=retrieved_at, finality_status="FINALIZED",
            daily=frames["day"], hourly=frames["60minute"],
            daily_content_hash=bars_content_hash(frames["day"]),
            hourly_content_hash=bars_content_hash(frames["60minute"]),
            finality_source=daily["finality_source"], finality_source_key=daily["finality_source_key"],
            finality_source_hash=daily["finality_source_hash"], finality_close=daily["finality_close"],
            finality_retrieved_at=pd.Timestamp(daily["finality_retrieved_at"]).to_pydatetime(),
        )
