#!/usr/bin/env python3
# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "pyarrow>=14.0",
# ]
# ///
"""
Generate per-entity JSON files from the parquet.

Reads sbac_data.parquet and writes one JSON file per entity into data/sbac_data/:
  state.json, county_{code}.json, district_{code}.json, school_{code}.json

Run after 1-ingest-original-files.py has produced the parquet.
"""

import json
from collections import defaultdict
from pathlib import Path

import pyarrow.compute as pc
import pyarrow.parquet as pq

BASE_DIR = Path(__file__).parent
PARQUET_PATH = BASE_DIR / "data" / "sbac_data.parquet"
OUT_DIR = BASE_DIR / "data" / "sbac_data"

KEEP_COLS = [
    "type_id", "county_code", "district_code", "school_code",
    "test_id", "grade", "year", "student_group_id", "student_group_name",
    "pct_met_and_above", "students_tested",
]
JSON_COLS = [
    "test_id", "grade", "year", "student_group_id", "student_group_name",
    "pct_met_and_above", "students_tested",
]


def table_to_records(table) -> list[dict]:
    """Convert a PyArrow table to a list of dicts (only JSON_COLS)."""
    cols = {name: table.column(name).to_pylist() for name in JSON_COLS}
    return [
        {k: cols[k][i] for k in JSON_COLS}
        for i in range(table.num_rows)
    ]


def write_json(records: list[dict], path: Path):
    with open(path, "w") as f:
        json.dump(records, f, separators=(",", ":"))


def process_type(table, type_id: int, group_col: str | None, prefix: str) -> int:
    """Filter to one type_id, group by entity code, write JSON files."""
    mask = pc.equal(table.column("type_id"), type_id)
    sub = table.filter(mask)

    # Drop rows without scores
    sub = sub.filter(pc.is_valid(sub.column("pct_met_and_above")))

    if sub.num_rows == 0:
        return 0

    # Sort
    sub = sub.sort_by([("test_id", "ascending"), ("grade", "ascending"),
                       ("year", "ascending"), ("student_group_id", "ascending")])

    if group_col is None:
        # State — single file
        write_json(table_to_records(sub), OUT_DIR / "state.json")
        return 1

    # Group by the entity code column
    codes = sub.column(group_col).to_pylist()
    # Build index of row ranges per code
    groups: dict[str, list[int]] = defaultdict(list)
    for i, code in enumerate(codes):
        groups[code].append(i)

    count = 0
    for code, indices in groups.items():
        chunk = sub.take(indices)
        write_json(table_to_records(chunk), OUT_DIR / f"{prefix}_{code}.json")
        count += 1

    return count


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Reading {PARQUET_PATH.name} (columns subset only) ...")
    table = pq.read_table(PARQUET_PATH, columns=KEEP_COLS)
    print(f"  {table.num_rows:,} rows loaded")

    specs = [
        (4, None, "state", "State"),
        (5, "county_code", "county", "Counties"),
        (6, "district_code", "district", "Districts"),
        (7, "school_code", "school", "Schools"),
        (9, "school_code", "school", "Schools (charter)"),
    ]

    total = 0
    for type_id, group_col, prefix, label in specs:
        n = process_type(table, type_id, group_col, prefix)
        print(f"  {label}: {n:,} files")
        total += n

    total_size = sum(f.stat().st_size for f in OUT_DIR.glob("*.json"))
    print(f"\nDone. {total:,} entity files → {total_size / 1_000_000:.1f} MB total")


if __name__ == "__main__":
    main()
