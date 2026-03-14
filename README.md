# California SBAC Scores

California CAASPP Smarter Balanced Assessment results for grades 3-8 and 11, from 2015 to 2025 (no 2020 due to COVID). Covers ELA and Math across all schools, districts, counties, and demographic subgroups.

~27 million rows ingested from [CAASPP research files](https://www.cde.ca.gov/ds/ad/filescaaspp.asp) into a single Parquet file, plus per-entity JSON files for lightweight frontend use.

## Data pipeline

```
original_zip_files/          Raw CAASPP zips (one per year)
        |
        v
1-ingest-original-files.py   Reads, normalizes, joins lookups -> data/sbac_data.parquet
        |
        v
1b-generate-entity-json.py   Splits parquet into ~12K per-entity JSON files -> data/sbac_data/
        |
        v
1c-upload-to-r2.sh           Syncs everything to Cloudflare R2
```

## What's on R2

| Path | Size | Description |
|---|---|---|
| `sbac_data.parquet` | 777 MB | Full dataset, sorted and row-grouped for DuckDB range requests |
| `sbac_entities.json` | 1.6 MB | Distinct entity list (for dropdowns) |
| `sbac_subgroups.json` | 8 KB | Demographic subgroup list |
| `sbac_data/state.json` | - | State-level scores |
| `sbac_data/county_{code}.json` | - | 58 county files |
| `sbac_data/district_{code}.json` | - | 983 district files |
| `sbac_data/school_{code}.json` | - | 11,039 school files |

## Querying with DuckDB

The parquet is sorted by `year, grade, student_group_id, type_id` with 100K-row groups, so DuckDB can efficiently skip irrelevant data via HTTP range requests:

```sql
INSTALL httpfs; LOAD httpfs;
SET s3_endpoint = '0ed4ae7d5070c68de6ce908a38ccc968.r2.cloudflarestorage.com';
SET s3_region = 'auto';

SELECT year, pct_met_and_above, students_tested
FROM read_parquet('s3://stuff/sbac_data.parquet')
WHERE type_id = 6 AND district_code = '10389'
  AND test_id = 1 AND student_group_id = 1 AND grade = 5
ORDER BY year;
```

## Key concepts

**Entity levels** (`type_id`): 4=State, 5=County, 6=District, 7/9=School. Every level has its own pre-aggregated rows -- don't sum school rows to get a district total.

**Student groups** (`student_group_id`): 1=All Students. Other IDs are demographic slices (Hispanic, Female, EL, etc.) that overlap -- don't sum across groups.

**Cohorts** (`cohort_year`): The year a cohort was in 3rd grade, calculated as `year - (grade - 3)`. Use this to track the same students across grades over time.

**Percentages**: `pct_met_and_above` is already a percentage. To aggregate across grades or entities, use a weighted average by `students_tested`.

## Running locally

```bash
# Ingest (takes ~5-10 min, needs ~8GB RAM)
uv run 1-ingest-original-files.py

# Generate per-entity JSON files
uv run 1b-generate-entity-json.py

# Upload to R2 (requires .env with R2 credentials)
./1c-upload-to-r2.sh

# Gradio analysis app
uv run 2-analyze.py
```

Scripts use [PEP 723](https://peps.python.org/pep-0723/) inline metadata, so `uv run` handles dependencies automatically. Do not use `uv add`.

## See also

- [AGENT_INSTRUCTIONS.md](AGENT_INSTRUCTIONS.md) -- detailed schema and query patterns for coding agents
- [CAASPP research files](https://www.cde.ca.gov/ds/ad/filescaaspp.asp) -- original data source
