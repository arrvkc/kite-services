#!/usr/bin/env python3
"""
Merge broker-exported historical tradebook CSV files into one canonical CSV.

Behavior:
- Validates required columns for the uploaded broker format
- Preserves all original columns
- Adds broker/account/source metadata columns
- Deduplicates by broker + account_id + trade_id
- Sorts deterministically by:
    1. order_execution_time
    2. trade_id
    3. order_id
- Writes one merged bootstrap CSV

Example:
python merge_historical_tradebooks.py \
  --broker zerodha \
  --account-id OMK569 \
  --output omk569_fo_historical_bootstrap.csv \
  "tradebook-OMK569-FO 24-25.csv" \
  "tradebook-OMK569-FO 25-25.csv" \
  "tradebook-OMK569-FO 26-27.csv"
"""

from __future__ import annotations

import argparse
import csv
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Tuple


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

# Keep the known broker columns first in the output if present.
PREFERRED_COLUMN_ORDER = [
    "symbol",
    "isin",
    "trade_date",
    "exchange",
    "segment",
    "series",
    "trade_type",
    "auction",
    "quantity",
    "price",
    "trade_id",
    "order_id",
    "order_execution_time",
    "expiry_date",
    "broker",
    "account_id",
    "source_file",
    "source_row_number",
    "source_unique_key",
]


@dataclass(frozen=True)
class ParsedRow:
    data: Dict[str, str]
    sort_key: Tuple[str, str, str]


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


def parse_timestamp(value: str) -> datetime:
    value = value.strip()
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        pass

    formats = [
        "%d-%m-%Y %H:%M:%S",
        "%d-%m-%Y %H:%M",
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y %H:%M",
        "%Y/%m/%d %H:%M:%S",
        "%Y/%m/%d %H:%M",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            pass
    raise ValueError(f"Unsupported timestamp format: {value}")


def validate_columns(fieldnames: Iterable[str], filepath: Path) -> None:
    cols = {normalize_header(c) for c in fieldnames if c is not None}
    missing = [c for c in REQUIRED_COLUMNS if c not in cols]
    if missing:
        raise ValueError(
            f"{filepath}: missing required columns: {', '.join(missing)}"
        )


def strict_side_check(trade_type: str, filepath: Path, row_number: int) -> None:
    side = trade_type.strip().upper()
    if side not in {"BUY", "SELL"}:
        raise ValueError(
            f"{filepath}: invalid trade_type '{trade_type}' at row {row_number}"
        )


def load_rows(
    filepath: Path,
    broker: str,
    account_id: str,
) -> List[ParsedRow]:
    parsed_rows: List[ParsedRow] = []

    with filepath.open("r", newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            raise ValueError(f"{filepath}: CSV has no header row")

        reader.fieldnames = [normalize_header(c) for c in reader.fieldnames]
        validate_columns(reader.fieldnames, filepath)

        for idx, raw_row in enumerate(reader, start=2):  # header is row 1
            row = {normalize_header(k): normalize_value(v) for k, v in raw_row.items() if k is not None}

            trade_id = row.get("trade_id", "").strip()
            if not trade_id:
                raise ValueError(f"{filepath}: blank trade_id at row {idx}")

            order_execution_time = row.get("order_execution_time", "").strip()
            if not order_execution_time:
                raise ValueError(
                    f"{filepath}: blank order_execution_time at row {idx}"
                )

            # Validate timestamp parseability.
            dt = parse_timestamp(order_execution_time)

            trade_type = row.get("trade_type", "")
            strict_side_check(trade_type, filepath, idx)

            quantity = row.get("quantity", "").strip()
            if not quantity:
                raise ValueError(f"{filepath}: blank quantity at row {idx}")

            price = row.get("price", "").strip()
            if not price:
                raise ValueError(f"{filepath}: blank price at row {idx}")

            order_id = row.get("order_id", "").strip()

            row["broker"] = broker
            row["account_id"] = account_id
            row["source_file"] = filepath.name
            row["source_row_number"] = str(idx)
            row["source_unique_key"] = build_source_unique_key(
                broker,
                account_id,
                trade_id,
                order_id,
                order_execution_time,
            )

            # Deterministic sort: timestamp, trade_id, order_id
            sort_key = (
                dt.isoformat(sep=" "),
                trade_id,
                order_id,
            )
            parsed_rows.append(ParsedRow(data=row, sort_key=sort_key))

    return parsed_rows


def build_output_columns(rows: List[ParsedRow]) -> List[str]:
    all_columns = set()
    for row in rows:
        all_columns.update(row.data.keys())

    ordered: List[str] = []
    for col in PREFERRED_COLUMN_ORDER:
        if col in all_columns:
            ordered.append(col)

    for col in sorted(all_columns):
        if col not in ordered:
            ordered.append(col)

    return ordered


def merge_tradebooks(
    input_files: List[Path],
    broker: str,
    account_id: str,
) -> Tuple[List[Dict[str, str]], int, int]:
    all_rows: List[ParsedRow] = []
    for path in input_files:
        all_rows.extend(load_rows(path, broker, account_id))

    # Sort first so the earliest deterministic occurrence wins if duplicates exist.
    all_rows.sort(key=lambda r: r.sort_key)

    deduped: List[Dict[str, str]] = []
    seen_keys = set()
    duplicate_count = 0

    for row in all_rows:
        unique_key = row.data["source_unique_key"]
        if unique_key in seen_keys:
            duplicate_count += 1
            continue
        seen_keys.add(unique_key)
        deduped.append(row.data)

    return deduped, len(all_rows), duplicate_count


def write_output(rows: List[Dict[str, str]], output_file: Path) -> None:
    output_columns = build_output_columns([ParsedRow(data=row, sort_key=("", "", "")) for row in rows])

    with output_file.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=output_columns, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Merge historical broker tradebook CSV files into one canonical bootstrap CSV."
    )
    parser.add_argument(
        "--broker",
        required=True,
        help="Broker name, e.g. zerodha",
    )
    parser.add_argument(
        "--account-id",
        required=True,
        help="Broker account/user identifier, e.g. OMK569",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Output merged CSV path",
    )
    parser.add_argument(
        "input_files",
        nargs="+",
        help="Input tradebook CSV files",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    input_files = [Path(p) for p in args.input_files]
    missing_files = [str(p) for p in input_files if not p.exists()]
    if missing_files:
        print("ERROR: Missing input file(s):")
        for path in missing_files:
            print(f"  - {path}")
        return 1

    output_file = Path(args.output)

    try:
        merged_rows, rows_seen, rows_ignored_duplicate = merge_tradebooks(
            input_files=input_files,
            broker=args.broker.strip(),
            account_id=args.account_id.strip(),
        )
        write_output(merged_rows, output_file)
    except Exception as e:
        print(f"ERROR: {e}")
        return 2

    print("SUCCESS: Historical tradebooks merged")
    print(f"output_file={output_file}")
    print(f"rows_seen={rows_seen}")
    print(f"rows_written={len(merged_rows)}")
    print(f"rows_ignored_duplicate={rows_ignored_duplicate}")
    return 0


if __name__ == "__main__":
    sys.exit(main())