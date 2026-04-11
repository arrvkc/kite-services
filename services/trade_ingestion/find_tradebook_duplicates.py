#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Tuple


REQUIRED_COLUMNS = [
    "symbol",
    "trade_date",
    "exchange",
    "segment",
    "trade_type",
    "quantity",
    "price",
    "trade_id",
    "order_id",
    "order_execution_time",
]


def normalize_header(name: str) -> str:
    return name.strip()


def normalize_value(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def build_source_unique_key(
    broker: str,
    account_id: str,
    trade_id: str,
    order_id: str,
    order_execution_time: str,
) -> str:
    return "|".join(
        [
            broker.strip(),
            account_id.strip(),
            trade_id.strip(),
            order_id.strip(),
            order_execution_time.strip(),
        ]
    )


def validate_columns(fieldnames, filepath: Path) -> None:
    cols = {normalize_header(c) for c in fieldnames if c is not None}
    missing = [c for c in REQUIRED_COLUMNS if c not in cols]
    if missing:
        raise ValueError(f"{filepath}: missing required columns: {', '.join(missing)}")


def canonical_row_fingerprint(row: Dict[str, str]) -> Tuple[Tuple[str, str], ...]:
    excluded = {"source_file", "source_row_number", "broker", "account_id", "source_unique_key"}
    return tuple(sorted((k, v) for k, v in row.items() if k not in excluded))


def load_rows(input_files: List[Path], broker: str, account_id: str):
    grouped = defaultdict(list)

    for filepath in input_files:
        with filepath.open("r", newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            if reader.fieldnames is None:
                raise ValueError(f"{filepath}: CSV has no header row")

            reader.fieldnames = [normalize_header(c) for c in reader.fieldnames]
            validate_columns(reader.fieldnames, filepath)

            for idx, raw_row in enumerate(reader, start=2):
                row = {
                    normalize_header(k): normalize_value(v)
                    for k, v in raw_row.items()
                    if k is not None
                }

                trade_id = row.get("trade_id", "").strip()
                order_id = row.get("order_id", "").strip()
                order_execution_time = row.get("order_execution_time", "").strip()

                if not trade_id:
                    raise ValueError(f"{filepath}: blank trade_id at row {idx}")
                if not order_id:
                    raise ValueError(f"{filepath}: blank order_id at row {idx}")
                if not order_execution_time:
                    raise ValueError(f"{filepath}: blank order_execution_time at row {idx}")

                row["broker"] = broker
                row["account_id"] = account_id
                row["source_file"] = filepath.name
                row["source_row_number"] = str(idx)
                row["source_unique_key"] = build_source_unique_key(
                    broker=broker,
                    account_id=account_id,
                    trade_id=trade_id,
                    order_id=order_id,
                    order_execution_time=order_execution_time,
                )

                grouped[row["source_unique_key"]].append(row)

    return grouped


def write_duplicates_report(grouped, output_file: Path) -> Tuple[int, int]:
    rows_out = []
    duplicate_groups = 0

    for source_unique_key, rows in grouped.items():
        if len(rows) <= 1:
            continue

        duplicate_groups += 1
        fingerprints = {canonical_row_fingerprint(r) for r in rows}
        identical_flag = "YES" if len(fingerprints) == 1 else "NO"

        for row in rows:
            rows_out.append(
                {
                    "source_unique_key": source_unique_key,
                    "trade_id": row.get("trade_id", ""),
                    "order_id": row.get("order_id", ""),
                    "symbol": row.get("symbol", ""),
                    "trade_type": row.get("trade_type", ""),
                    "quantity": row.get("quantity", ""),
                    "price": row.get("price", ""),
                    "order_execution_time": row.get("order_execution_time", ""),
                    "trade_date": row.get("trade_date", ""),
                    "exchange": row.get("exchange", ""),
                    "segment": row.get("segment", ""),
                    "source_file": row.get("source_file", ""),
                    "source_row_number": row.get("source_row_number", ""),
                    "duplicate_group_size": str(len(rows)),
                    "all_rows_identical": identical_flag,
                }
            )

    fieldnames = [
        "source_unique_key",
        "trade_id",
        "order_id",
        "symbol",
        "trade_type",
        "quantity",
        "price",
        "order_execution_time",
        "trade_date",
        "exchange",
        "segment",
        "source_file",
        "source_row_number",
        "duplicate_group_size",
        "all_rows_identical",
    ]

    with output_file.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows_out)

    return duplicate_groups, len(rows_out)


def parse_args():
    parser = argparse.ArgumentParser(description="Find duplicate trades across historical tradebook CSV files.")
    parser.add_argument("--broker", required=True)
    parser.add_argument("--account-id", required=True)
    parser.add_argument("--output", required=True, help="Duplicate report CSV path")
    parser.add_argument("input_files", nargs="+")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    input_files = [Path(p) for p in args.input_files]

    missing_files = [str(p) for p in input_files if not p.exists()]
    if missing_files:
        print("ERROR: Missing input files:")
        for path in missing_files:
            print(f"  - {path}")
        return 1

    try:
        grouped = load_rows(input_files, args.broker.strip(), args.account_id.strip())
        duplicate_groups, duplicate_rows = write_duplicates_report(grouped, Path(args.output))
    except Exception as e:
        print(f"ERROR: {e}")
        return 2

    print("SUCCESS: Duplicate report generated")
    print(f"output_file={args.output}")
    print(f"duplicate_groups={duplicate_groups}")
    print(f"duplicate_rows={duplicate_rows}")
    return 0


if __name__ == "__main__":
    sys.exit(main())