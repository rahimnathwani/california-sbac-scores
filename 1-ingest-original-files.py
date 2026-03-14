#!/usr/bin/env python3
# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "pandas>=2.0",
#   "pyarrow>=14.0",
# ]
# ///
"""
Ingest CAASPP SBAC research files into a single parquet file for analysis.

Handles:
- 2015-2019: comma-delimited, no District/School names in main file
- 2021-2023: caret-delimited, no District/School names in main file
- 2024-2025: caret-delimited, District/School names inline
"""

import io
import zipfile
from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).parent
ALL_STUDENT_GROUPS_DIR = BASE_DIR / "original_zip_files" / "all-student-groups"
FILE_FORMATS_DIR = BASE_DIR / "original_zip_files" / "file_formats"
OUTPUT_PATH = BASE_DIR / "data" / "sbac_data.parquet"

YEARS = [2015, 2016, 2017, 2018, 2019, 2021, 2022, 2023, 2024, 2025]
GRADES_TO_KEEP = {3, 4, 5, 6, 7, 8, 11}
TEST_TYPE_SB = "B"
TEST_IDS_TO_KEEP = {1, 2}  # 1=ELA, 2=Math

# Column name normalisation: maps raw column names → standard name
# Each year's main file has different column names for the same concept
COLUMN_ALIASES = {
    # Student group / subgroup ID
    "Subgroup ID": "student_group_id",          # 2015-2019
    "Student Group ID": "student_group_id",     # 2021-2025
    # Test ID
    "Test Id": "test_id",                        # 2015-2019
    "Test ID": "test_id",                        # 2021-2025
    # Shared columns
    "County Code": "county_code",
    "District Code": "district_code",
    "School Code": "school_code",
    "Test Year": "year",
    "Test Type": "test_type",
    "Grade": "grade",
    "Students Tested": "students_tested",               # 2015-2023
    "Total Students Tested": "students_tested",         # 2024-2025
    "Percentage Standard Met and Above": "pct_met_and_above",
    # 2024-2025 inline names
    "District Name": "district_name",
    "School Name": "school_name",
    # Type ID
    "Type ID": "type_id",
    "Type Id": "type_id",
}

COLUMNS_TO_KEEP = [
    "year", "county_code", "district_code", "school_code",
    "test_type", "test_id", "student_group_id", "grade",
    "students_tested", "pct_met_and_above",
    # optional, filled later if not present
]


def read_delimited_from_zip(zf: zipfile.ZipFile, filename: str) -> pd.DataFrame:
    """Read a comma or caret-delimited file from a zip archive."""
    with zf.open(filename) as f:
        raw = f.read(512)  # sniff delimiter
        delimiter = "^" if b"^" in raw else ","
        f2 = io.BytesIO(raw + f.read())
        return pd.read_csv(
            f2,
            sep=delimiter,
            dtype=str,
            low_memory=False,
            encoding="latin-1",
        )


def read_entities(zf: zipfile.ZipFile, year: int) -> pd.DataFrame:
    """Read entities file from within the main zip."""
    candidates = [n for n in zf.namelist() if "entities" in n.lower()]
    if not candidates:
        return pd.DataFrame()
    df = read_delimited_from_zip(zf, candidates[0])
    df.columns = [c.strip() for c in df.columns]
    # Normalise column names
    df = df.rename(columns={
        "County Code": "county_code",
        "District Code": "district_code",
        "School Code": "school_code",
        "County Name": "county_name",
        "District Name": "district_name",
        "School Name": "school_name",
        "Type Id": "type_id",
        "Type ID": "type_id",
    })
    keep = ["county_code", "district_code", "school_code",
            "county_name", "district_name", "school_name", "type_id"]
    keep = [c for c in keep if c in df.columns]
    df = df[keep].drop_duplicates(subset=["county_code", "district_code", "school_code"])
    return df


def process_year(year: int) -> pd.DataFrame:
    zip_path = next(ALL_STUDENT_GROUPS_DIR.glob(f"sb_ca{year}_all_csv_v*.zip"))
    print(f"  Reading {zip_path.name} ...", flush=True)

    with zipfile.ZipFile(zip_path) as zf:
        # Find the main data file (not entities)
        data_files = [n for n in zf.namelist()
                      if n.endswith(".txt") and "entities" not in n.lower()]
        if not data_files:
            raise FileNotFoundError(f"No main data file found in {zip_path}")
        data_filename = data_files[0]

        df = read_delimited_from_zip(zf, data_filename)
        df.columns = [c.strip() for c in df.columns]

        # Normalise column names
        df = df.rename(columns={k: v for k, v in COLUMN_ALIASES.items() if k in df.columns})

        # Filter early to reduce memory
        df["test_type"] = df["test_type"].str.strip()
        df = df[df["test_type"] == TEST_TYPE_SB]

        df["test_id"] = pd.to_numeric(df["test_id"], errors="coerce")
        df = df[df["test_id"].isin(TEST_IDS_TO_KEEP)]

        df["grade"] = pd.to_numeric(df["grade"], errors="coerce")
        df = df[df["grade"].isin(GRADES_TO_KEEP)]

        df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int16")
        df["student_group_id"] = pd.to_numeric(df["student_group_id"], errors="coerce").astype("Int16")

        # pct_met_and_above: may be "*", empty, or float
        df["pct_met_and_above"] = pd.to_numeric(df["pct_met_and_above"], errors="coerce")
        df["students_tested"] = pd.to_numeric(df["students_tested"], errors="coerce").astype("Int32")

        # Read entities for name lookup
        entities = read_entities(zf, year)

    # For 2024-2025, district_name and school_name are already inline
    has_inline_names = "district_name" in df.columns and "school_name" in df.columns

    if has_inline_names:
        # county_name not inline; get from entities
        if not entities.empty and "county_name" in entities.columns:
            county_map = (
                entities[["county_code", "county_name"]]
                .drop_duplicates("county_code")
                .set_index("county_code")["county_name"]
            )
            df["county_name"] = df["county_code"].map(county_map)
        else:
            df["county_name"] = ""

        # type_id: inline in 2024-2025
        if "type_id" in df.columns:
            df["type_id"] = pd.to_numeric(df["type_id"], errors="coerce").astype("Int8")
        elif not entities.empty and "type_id" in entities.columns:
            type_map = (
                entities.set_index(["county_code", "district_code", "school_code"])["type_id"]
            )
            df["type_id"] = (
                df.set_index(["county_code", "district_code", "school_code"])
                .index.map(type_map)
            )
    else:
        # Join entities for names and type_id
        if not entities.empty:
            df = df.merge(
                entities,
                on=["county_code", "district_code", "school_code"],
                how="left",
            )
            if "type_id" in df.columns:
                df["type_id"] = pd.to_numeric(df["type_id"], errors="coerce").astype("Int8")
        else:
            df["county_name"] = ""
            df["district_name"] = ""
            df["school_name"] = ""
            df["type_id"] = pd.NA

        # 2015-2019 have type_id in the entities file (not main file)
        # if still missing, infer from codes
        if "type_id" not in df.columns:
            df["type_id"] = pd.NA

    # Infer type_id where missing from codes
    if "type_id" not in df.columns or df["type_id"].isna().any():
        mask_missing = df.get("type_id", pd.Series(pd.NA, index=df.index)).isna()
        if mask_missing.any():
            inferred = df.loc[mask_missing].apply(_infer_type_id, axis=1).astype("Int8")
            if "type_id" not in df.columns:
                df["type_id"] = inferred
            else:
                df.loc[mask_missing, "type_id"] = inferred

    # Fill missing name columns
    for col in ["county_name", "district_name", "school_name"]:
        if col not in df.columns:
            df[col] = ""
        else:
            df[col] = df[col].fillna("").str.strip()

    print(f"    → {len(df):,} rows after filtering", flush=True)
    return df


def _infer_type_id(row) -> int:
    """Infer entity type from county/district/school codes."""
    if row["district_code"] == "00000":
        return 4 if row["county_code"] == "00" else 5
    elif row["school_code"] == "0000000":
        return 6
    else:
        return 7


def load_student_groups() -> pd.DataFrame:
    """Load student group lookup from the most recent available year."""
    all_groups = []
    for year in reversed(YEARS):
        sg_path = FILE_FORMATS_DIR / str(year) / "StudentGroups.zip"
        if not sg_path.exists():
            continue
        with zipfile.ZipFile(sg_path) as zf:
            fname = zf.namelist()[0]
            df = read_delimited_from_zip(zf, fname)
        df.columns = [c.strip() for c in df.columns]
        # Columns: Demographic ID, Demographic ID Num, Demographic Name, Student Group
        num_col = next((c for c in df.columns if "Num" in c), None)
        name_col = next((c for c in df.columns if "Demographic Name" in c), None)
        cat_col = next((c for c in df.columns if "Student Group" in c and "ID" not in c), None)
        if num_col and name_col:
            sub = df[[num_col, name_col]].copy()
            if cat_col:
                sub["student_group_category"] = df[cat_col]
            else:
                sub["student_group_category"] = ""
            sub.columns = ["student_group_id", "student_group_name", "student_group_category"]
            sub["student_group_id"] = pd.to_numeric(sub["student_group_id"], errors="coerce")
            all_groups.append(sub.dropna(subset=["student_group_id"]))
    if not all_groups:
        return pd.DataFrame(columns=["student_group_id", "student_group_name", "student_group_category"])
    combined = pd.concat(all_groups, ignore_index=True)
    combined = combined.drop_duplicates(subset=["student_group_id"])
    combined["student_group_id"] = combined["student_group_id"].astype("Int16")
    return combined


def load_tests() -> pd.DataFrame:
    """Load test ID → name lookup."""
    for year in reversed(YEARS):
        t_path = FILE_FORMATS_DIR / str(year) / "Tests.zip"
        if not t_path.exists():
            continue
        with zipfile.ZipFile(t_path) as zf:
            fname = zf.namelist()[0]
            df = read_delimited_from_zip(zf, fname)
        df.columns = [c.strip() for c in df.columns]
        num_col = next((c for c in df.columns if "Num" in c or "ID Num" in c), None)
        name_col = next((c for c in df.columns if "Name" in c), None)
        if num_col and name_col:
            df = df[[num_col, name_col]].copy()
            df.columns = ["test_id", "test_name"]
            df["test_id"] = pd.to_numeric(df["test_id"], errors="coerce")
            return df.dropna(subset=["test_id"])
    return pd.DataFrame(columns=["test_id", "test_name"])


def main():
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    print("Loading lookup tables...")
    student_groups = load_student_groups()
    print(f"  {len(student_groups)} student groups loaded")
    tests = load_tests()
    print(f"  {len(tests)} test IDs loaded: {tests[tests['test_id'].isin([1,2])][['test_id','test_name']].to_dict('records')}")

    all_dfs = []
    for year in YEARS:
        print(f"\nYear {year}:")
        try:
            df = process_year(year)
            all_dfs.append(df)
        except Exception as e:
            print(f"  ERROR: {e}")

    print("\nConcatenating all years...")
    combined = pd.concat(all_dfs, ignore_index=True)
    print(f"Total rows before dedup: {len(combined):,}")

    # Join student group names
    student_groups["student_group_id"] = student_groups["student_group_id"].astype("Int16")
    combined["student_group_id"] = combined["student_group_id"].astype("Int16")
    combined = combined.merge(student_groups, on="student_group_id", how="left")

    # Join test names
    tests["test_id"] = tests["test_id"].astype("Int8")
    combined["test_id"] = combined["test_id"].astype("Int8")
    combined = combined.merge(tests, on="test_id", how="left")

    # Final type cleanup
    combined["grade"] = combined["grade"].astype("Int8")
    combined["year"] = combined["year"].astype("Int16")
    combined["type_id"] = pd.to_numeric(combined["type_id"], errors="coerce").astype("Int8")

    # Add cohort year: year when this cohort was in 3rd grade
    # cohort_year = test_year - (grade - 3)
    combined["cohort_year"] = (combined["year"] - (combined["grade"] - 3)).astype("Int16")

    print(f"\nFinal dataset: {len(combined):,} rows")
    print(f"Years: {sorted(combined['year'].dropna().unique().tolist())}")
    print(f"Grades: {sorted(combined['grade'].dropna().unique().tolist())}")
    print(f"Tests: {combined[['test_id','test_name']].drop_duplicates().sort_values('test_id').to_dict('records')}")
    print(f"Student groups: {len(combined['student_group_id'].unique())}")

    # Sort for range-request efficiency — DuckDB uses Parquet row-group
    # min/max statistics for predicate pushdown over HTTP range requests.
    print("\nSorting for range-request efficiency...")
    combined = combined.sort_values(
        ["year", "grade", "student_group_id", "type_id", "district_code", "school_code"],
        na_position="first",
    ).reset_index(drop=True)

    print(f"Saving to {OUTPUT_PATH} ...")
    combined.to_parquet(
        OUTPUT_PATH,
        index=False,
        engine="pyarrow",
        compression="zstd",
        row_group_size=100_000,
    )
    print(f"Done. File size: {OUTPUT_PATH.stat().st_size / 1_000_000:.1f} MB")

    # -- Sidecar JSON files for UI dropdowns / entity lookups ---------------

    # Entities: distinct list from 2024, student_group_id=1, grade=3
    print("\nGenerating sbac_entities.json ...")
    entities_df = combined[
        (combined["student_group_id"] == 1)
        & (combined["grade"] == 3)
        & (combined["year"] == 2024)
    ][["type_id", "county_code", "county_name", "district_code",
       "district_name", "school_code", "school_name"]].drop_duplicates()
    entities_df = entities_df.sort_values(
        ["type_id", "county_name", "district_name", "school_name"]
    )
    entities_path = OUTPUT_PATH.parent / "sbac_entities.json"
    entities_df.to_json(entities_path, orient="records", indent=2)
    print(f"  {len(entities_df):,} entities → {entities_path.stat().st_size / 1_000:.1f} KB")

    # Subgroups: distinct list (excluding "All Students")
    print("Generating sbac_subgroups.json ...")
    sg_df = combined[
        (combined["type_id"] == 4)
        & (combined["grade"] == 3)
        & (combined["year"] == 2024)
        & (combined["student_group_id"] != 1)
    ][["student_group_id", "student_group_name", "student_group_category"]].drop_duplicates()
    sg_df = sg_df.sort_values(["student_group_category", "student_group_name"])
    sg_path = OUTPUT_PATH.parent / "sbac_subgroups.json"
    sg_df.to_json(sg_path, orient="records", indent=2)
    print(f"  {len(sg_df)} subgroups → {sg_path.stat().st_size / 1_000:.1f} KB")


if __name__ == "__main__":
    main()
