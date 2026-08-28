from __future__ import annotations

import argparse
import csv
import os
import time
from datetime import date
from pathlib import Path

import pandas as pd
import psycopg2

from engines.strategy_deterministic_engine.adapters.trend_identifier_batch_adapter import (
    TrendIdentifierBatchAdapter,
)
from engines.strategy_deterministic_engine.engine import evaluate_batch
from engines.strategy_deterministic_engine.scripts.run_strategy_engine_batch_from_db import (
    output_to_row,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate FO universe strategy validation CSV using synthetic contract data."
    )
    parser.add_argument(
        "--output",
        default="data/strategy_validation/fo_universe_validation.csv",
    )
    parser.add_argument(
        "--symbols",
        default="",
        help="Optional comma-separated symbols. Example: MCX,BSE,TCS",
    )
    parser.add_argument("--history-days", type=int, default=5)
    parser.add_argument("--forward-days", type=int, default=5)
    parser.add_argument("--near-dte", type=int, default=20)
    parser.add_argument("--next-dte", type=int, default=40)
    parser.add_argument(
        "--through-date",
        default="",
        help="Optional inclusive upper bound for recovery input, e.g. 2026-08-27",
    )
    return parser.parse_args()


def fetch_trend_history(symbols: list[str] | None) -> pd.DataFrame:
    db_url = os.environ["ATMS_DATABASE_URL"]

    conn = psycopg2.connect(db_url)
    cur = conn.cursor()

    if symbols:
        cur.execute(
            """
            SELECT
                symbol,
                trade_date AS date,
                close,
                label,
                confidence,
                aggregate_score,
                internal_state,
                exchange,
                tradingsymbol,
                instrument_token
            FROM trend_history_fo_universe
            WHERE symbol = ANY(%s)
            ORDER BY symbol, trade_date;
            """,
            (symbols,),
        )
    else:
        cur.execute(
            """
            SELECT
                symbol,
                trade_date AS date,
                close,
                label,
                confidence,
                aggregate_score,
                internal_state,
                exchange,
                tradingsymbol,
                instrument_token
            FROM trend_history_fo_universe
            ORDER BY symbol, trade_date;
            """
        )

    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]

    cur.close()
    conn.close()

    df = pd.DataFrame(rows, columns=cols)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    return df


def bound_trend_history(
    trend_df: pd.DataFrame,
    through_date: date | None,
) -> pd.DataFrame:
    if through_date is None:
        return trend_df
    return trend_df.loc[trend_df["date"] <= through_date].copy()


def main() -> None:
    args = parse_args()
    through_date = date.fromisoformat(args.through_date) if args.through_date else None

    requested_symbols = (
        [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
        if args.symbols
        else None
    )

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    trend_df = bound_trend_history(
        fetch_trend_history(requested_symbols),
        through_date,
    )
    symbols = sorted(trend_df["symbol"].dropna().unique())

    rows: list[dict] = []
    started = time.time()
    eval_count = 0

    for si, symbol in enumerate(symbols, start=1):
        sym_df = (
            trend_df[trend_df["symbol"] == symbol]
            .sort_values("date")
            .reset_index(drop=True)
        )

        if len(sym_df) < args.history_days + args.forward_days:
            continue

        print(f"[{si}/{len(symbols)}] {symbol} rows={len(sym_df)}")

        for i in range(len(sym_df)):
            hist = sym_df.iloc[: i + 1].tail(args.history_days).copy()

            if len(hist) < args.history_days:
                continue

            run_date = hist.iloc[-1]["date"]

            contract_df = pd.DataFrame(
                [
                    {
                        "symbol": symbol,
                        "selection_date": run_date,
                        "near_expiry": run_date,
                        "next_expiry": run_date,
                        "dte_near_month": args.near_dte,
                        "next_month_available": True,
                        "dte_next_month": args.next_dte,
                    }
                ]
            )

            try:
                strategy_inputs = TrendIdentifierBatchAdapter.from_dataframes(
                    trend_history_df=hist,
                    contract_snapshot_df=contract_df,
                ).build_all()

                if not strategy_inputs:
                    continue

                batch_result = evaluate_batch(strategy_inputs)
                input_map = {str(item.instrument): item for item in strategy_inputs}

                for item in batch_result["results"]:
                    if item["mode"] != "public_payload":
                        continue

                    payload = item["payload"]

                    if payload["instrument"] != symbol:
                        continue

                    out_row = output_to_row(payload, input_map[symbol])

                    signal_close = sym_df.iloc[i]["close"]
                    future_index = i + args.forward_days
                    close_forward = (
                        sym_df.iloc[future_index]["close"]
                        if future_index < len(sym_df)
                        else None
                    )
                    return_forward_pct = (
                        ((close_forward - signal_close) / signal_close) * 100
                        if close_forward is not None
                        else None
                    )

                    rows.append(
                        {
                            "run_date": run_date.isoformat(),
                            "symbol": symbol,
                            "close": signal_close,
                            "label": out_row[1],
                            "score": out_row[2],
                            "confidence": out_row[3],
                            "state": out_row[4],
                            "bull5": out_row[5],
                            "bear5": out_row[6],
                            "flat5": out_row[7],
                            "signflip5": out_row[8],
                            "mean3": out_row[9],
                            "regime": out_row[12],
                            "candidate_family": out_row[13],
                            "strategy_family": out_row[14],
                            "contract_month": out_row[15],
                            "strength": out_row[16],
                            "top_n": out_row[17],
                            "rank_all": out_row[18],
                            "rank_family": out_row[19],
                            "transition_state": out_row[20],
                            f"close_{args.forward_days}d": close_forward,
                            f"return_{args.forward_days}d_pct": return_forward_pct,
                        }
                    )

                    eval_count += 1

            except Exception as exc:
                print(f"ERROR {symbol} {run_date}: {type(exc).__name__}: {exc}")

        if si % 20 == 0:
            elapsed = time.time() - started
            print(
                f"Progress: symbols={si}, evaluations={eval_count}, elapsed_sec={elapsed:.1f}"
            )

    if not rows:
        raise SystemExit("No rows generated")

    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)

    print("")
    print("=" * 80)
    print(f"Saved: {output_path}")
    print(f"Rows : {len(rows)}")
    print("=" * 80)


if __name__ == "__main__":
    main()
