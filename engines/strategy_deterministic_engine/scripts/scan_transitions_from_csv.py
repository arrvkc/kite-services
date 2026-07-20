from __future__ import annotations

import argparse
import sys
from datetime import timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from kiteconnect import KiteConnect  # noqa: E402
from services.kite_credentials_service import get_kite_credentials  # noqa: E402


BASE_OUTPUT_COLUMNS = [
    "run_date",
    "symbol",
    "event_type",
    "change_types",
    "previous_regime_bucket",
    "current_regime_bucket",
    "previous_strategy_family",
    "current_strategy_family",
    "previous_strength",
    "current_strength",
    "strength_delta",
    "previous_confidence",
    "current_confidence",
    "confidence_delta",
    "previous_transition_state",
    "current_transition_state",
    "previous_include_in_top_n",
    "current_include_in_top_n",
    "reason_codes",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scan strategy deterministic engine CSV for regime/strategy transitions."
    )
    parser.add_argument("--input-csv", required=True)
    parser.add_argument("--output-csv", required=True)
    parser.add_argument("--run-date", default="", help="Optional date filter, e.g. 2026-05-22")
    parser.add_argument("--user-id", default="", help="Kite user id. If provided, enriches price/volume.")
    parser.add_argument("--min-strength-delta", type=float, default=15.0)
    parser.add_argument("--min-confidence-delta", type=float, default=0.05)
    return parser.parse_args()


def normalize_bool(value: Any) -> bool:
    return str(value).strip().lower() in {"t", "true", "1", "yes"}


def build_kite_client(user_id: str) -> KiteConnect:
    api_key, access_token = get_kite_credentials(user_id)
    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    return kite


def resolve_nse_equity_token(kite: KiteConnect, symbol: str, instruments_df: pd.DataFrame) -> Optional[int]:
    symbol = symbol.upper().strip()

    exact = instruments_df[
        (instruments_df["tradingsymbol"].astype(str).str.upper() == symbol)
        & (instruments_df["exchange"].astype(str).str.upper() == "NSE")
    ].copy()

    if exact.empty:
        return None

    if "instrument_type" in exact.columns:
        eq = exact[exact["instrument_type"].fillna("").astype(str).str.upper() == "EQ"].copy()
        if not eq.empty:
            exact = eq

    return int(exact.iloc[0]["instrument_token"])


def fetch_price_volume_context(
    kite: KiteConnect,
    instruments_df: pd.DataFrame,
    symbol: str,
    run_date: pd.Timestamp,
) -> Dict[str, Any]:
    token = resolve_nse_equity_token(kite, symbol, instruments_df)

    if token is None:
        return {
            "close": None,
            "previous_close": None,
            "price_change_pct": None,
            "volume": None,
            "avg_volume_20": None,
            "volume_ratio_20d": None,
            "price_volume_error": f"Could not resolve NSE equity token for {symbol}",
        }

    from_dt = (run_date - timedelta(days=45)).to_pydatetime()
    to_dt = (run_date + timedelta(days=1)).to_pydatetime()

    candles = kite.historical_data(
        instrument_token=token,
        from_date=from_dt,
        to_date=to_dt,
        interval="day",
        continuous=False,
        oi=False,
    )

    df = pd.DataFrame(candles)

    if df.empty:
        return {
            "close": None,
            "previous_close": None,
            "price_change_pct": None,
            "volume": None,
            "avg_volume_20": None,
            "volume_ratio_20d": None,
            "price_volume_error": f"No daily candles for {symbol}",
        }

    df = df.rename(columns={"date": "timestamp"})
    df["timestamp"] = pd.to_datetime(df["timestamp"]).dt.tz_localize(None)
    df["trade_date"] = df["timestamp"].dt.date
    df = df.sort_values("timestamp").reset_index(drop=True)

    same_day = df[df["trade_date"] == run_date.date()].copy()

    if same_day.empty:
        return {
            "close": None,
            "previous_close": None,
            "price_change_pct": None,
            "volume": None,
            "avg_volume_20": None,
            "volume_ratio_20d": None,
            "price_volume_error": f"No candle for {symbol} on {run_date.date()}",
        }

    idx = same_day.index[-1]
    current = df.loc[idx]

    previous_rows = df.loc[: idx - 1].tail(20)
    previous_close = previous_rows.iloc[-1]["close"] if not previous_rows.empty else None
    avg_volume_20 = previous_rows["volume"].mean() if not previous_rows.empty else None

    close = float(current["close"])
    volume = float(current["volume"])

    price_change_pct = (
        ((close - float(previous_close)) / float(previous_close)) * 100
        if previous_close not in [None, 0]
        else None
    )

    volume_ratio_20d = (
        volume / float(avg_volume_20)
        if avg_volume_20 not in [None, 0]
        else None
    )

    return {
        "close": round(close, 2),
        "previous_close": round(float(previous_close), 2) if previous_close is not None else None,
        "price_change_pct": round(price_change_pct, 2) if price_change_pct is not None else None,
        "volume": int(volume),
        "avg_volume_20": round(float(avg_volume_20), 2) if avg_volume_20 is not None else None,
        "volume_ratio_20d": round(volume_ratio_20d, 2) if volume_ratio_20d is not None else None,
        "price_volume_error": "",
    }


def main() -> None:
    args = parse_args()

    df = pd.read_csv(args.input_csv)
    df["run_date"] = pd.to_datetime(df["run_date"])
    df["include_in_top_n"] = df["include_in_top_n"].map(normalize_bool)

    numeric_cols = ["final_strategy_strength", "confidence"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.sort_values(["symbol", "run_date"]).reset_index(drop=True)

    rows: List[Dict[str, Any]] = []

    for symbol, g in df.groupby("symbol"):
        g = g.sort_values("run_date").reset_index(drop=True)

        for i in range(1, len(g)):
            prev = g.iloc[i - 1]
            curr = g.iloc[i]

            if args.run_date and curr["run_date"].date() != pd.Timestamp(args.run_date).date():
                continue

            change_types: List[str] = []

            if prev["strategy_family"] != curr["strategy_family"]:
                change_types.append("STRATEGY_FAMILY_CHANGE")

            if prev["regime_bucket"] != curr["regime_bucket"]:
                change_types.append("REGIME_BUCKET_CHANGE")

            if bool(prev["include_in_top_n"]) != bool(curr["include_in_top_n"]):
                change_types.append(
                    "TOPN_ENTERED" if curr["include_in_top_n"] else "TOPN_EXITED"
                )

            strength_delta = curr["final_strategy_strength"] - prev["final_strategy_strength"]
            confidence_delta = curr["confidence"] - prev["confidence"]

            if pd.notna(strength_delta) and abs(strength_delta) >= args.min_strength_delta:
                change_types.append("STRENGTH_JUMP")

            if pd.notna(confidence_delta) and abs(confidence_delta) >= args.min_confidence_delta:
                change_types.append("CONFIDENCE_JUMP")

            if prev["strategy_transition_state"] != curr["strategy_transition_state"]:
                change_types.append("TRANSITION_STATE_CHANGE")

            event_type = "TRANSITION" if change_types else "NON_TRANSITION"

            rows.append(
                {
                    "run_date": curr["run_date"].date().isoformat(),
                    "symbol": symbol,
                    "event_type": event_type,
                    "change_types": ",".join(change_types),
                    "previous_regime_bucket": prev["regime_bucket"],
                    "current_regime_bucket": curr["regime_bucket"],
                    "previous_strategy_family": prev["strategy_family"],
                    "current_strategy_family": curr["strategy_family"],
                    "previous_strength": prev["final_strategy_strength"],
                    "current_strength": curr["final_strategy_strength"],
                    "strength_delta": round(strength_delta, 2) if pd.notna(strength_delta) else None,
                    "previous_confidence": prev["confidence"],
                    "current_confidence": curr["confidence"],
                    "confidence_delta": round(confidence_delta, 4) if pd.notna(confidence_delta) else None,
                    "previous_transition_state": prev["strategy_transition_state"],
                    "current_transition_state": curr["strategy_transition_state"],
                    "previous_include_in_top_n": prev["include_in_top_n"],
                    "current_include_in_top_n": curr["include_in_top_n"],
                    "reason_codes": curr.get("reason_codes", ""),
                }
            )

    out = pd.DataFrame(rows, columns=BASE_OUTPUT_COLUMNS)

    if out.empty:
        print("No meaningful transitions found.")
        Path(args.output_csv).parent.mkdir(parents=True, exist_ok=True)
        out.to_csv(args.output_csv, index=False)
        return

    if args.user_id:
        kite = build_kite_client(args.user_id)
        instruments_df = pd.DataFrame(kite.instruments("NSE"))

        enriched_rows = []

        for row in out.to_dict("records"):
            context = fetch_price_volume_context(
                kite=kite,
                instruments_df=instruments_df,
                symbol=row["symbol"],
                run_date=pd.Timestamp(row["run_date"]),
            )
            row.update(context)
            enriched_rows.append(row)

        out = pd.DataFrame(enriched_rows)

        def _keep_signal_row(row):
            if row.get("event_type") == "TRANSITION":
                return True

            vol = row.get("volume_ratio_20d")
            pchg = row.get("price_change_pct")

            high_vol = pd.notna(vol) and float(vol) >= 1.5
            strong_price = pd.notna(pchg) and abs(float(pchg)) >= 2

            return high_vol or strong_price

        out = out[out.apply(_keep_signal_row, axis=1)].copy()

    out = out.sort_values(["run_date", "event_type", "change_types", "symbol"]).reset_index(drop=True)

    output_path = Path(args.output_csv)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(output_path, index=False)

    print(out.to_string(index=False))
    print()
    print(f"Saved CSV: {output_path}")


if __name__ == "__main__":
    main()
