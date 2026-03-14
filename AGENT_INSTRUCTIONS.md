# CAASPP SBAC Data — Agent Instructions

## Data source

A single Parquet file (`sbac_data.parquet`) containing California CAASPP Smarter Balanced Assessment results for grades 3–8 and 11, years 2015–2025 (no 2020), covering both ELA and Math. It has ~27 million rows and is hosted on Cloudflare R2. Query it with DuckDB over HTTP range requests — only the relevant row groups will be fetched.

## Schema (key columns)

| Column | Type | Notes |
|---|---|---|
| `year` | int | Test year (2015–2025, no 2020) |
| `type_id` | int | Entity level: 4=State, 5=County, 6=District, 7/9=School |
| `county_code/name` | str | |
| `district_code/name` | str | |
| `school_code/name` | str | |
| `test_id` | int | 1=ELA, 2=Math |
| `test_name` | str | |
| `student_group_id` | int | 1=All Students; others are demographic subgroups |
| `student_group_name` | str | |
| `student_group_category` | str | e.g. "Gender", "Race/Ethnicity", "English Learners" |
| `grade` | int | 3–8, 11 |
| `pct_met_and_above` | float | % of students meeting or exceeding standard (0–100); null if suppressed |
| `students_tested` | int | Headcount for this row |
| `cohort_year` | int | Year this cohort was in 3rd grade: `year - (grade - 3)` |

## The aggregation trap — do NOT sum rows

**Every level of aggregation already has its own row.** The state total, each county total, each district total, and each school result are all separate rows in the same table. If you sum or count rows naively, you will multiply-count students.

Rules:
- To get a **state** result: filter `type_id = 4`. Do not sum district or school rows.
- To get a **district** result: filter `type_id = 6` and the district code. Do not sum its school rows.
- To get **all students**: filter `student_group_id = 1`. Do not sum across student groups — every subgroup (Hispanic, Female, EL, etc.) is an independent slice of the same students.
- `pct_met_and_above` is a **percentage**, not a count. If you need to aggregate it across grades or years, use a **weighted average** weighted by `students_tested`, not a simple mean.

## Cohorts

A cohort is identified by `cohort_year` — the year that cohort was in 3rd grade. Each cohort can be tracked longitudinally as they move through grades:

- Cohort 2015 was in grade 3 in 2015, grade 4 in 2016, …, grade 8 in 2021, grade 11 in 2023
- Formula: `cohort_year = year - (grade - 3)`

To follow a cohort over time, filter on `cohort_year` and sort by `year` (or equivalently `grade`). Note that grade 11 has a gap because the cohort skips grades 9 and 10 (not tested).

## Common filter patterns

```sql
-- State ELA trend, all students, grade 5
SELECT year, pct_met_and_above, students_tested
FROM read_parquet('s3://stuff/sbac_data.parquet')
WHERE type_id = 4
  AND test_id = 1
  AND student_group_id = 1
  AND grade = 5
ORDER BY year;

-- Single cohort (entered 3rd grade in 2015), all grades, ELA, district X
SELECT grade, year, pct_met_and_above
FROM read_parquet('s3://stuff/sbac_data.parquet')
WHERE cohort_year = 2015
  AND district_code = '...'
  AND type_id = 6
  AND test_id = 1
  AND student_group_id = 1
ORDER BY grade;

-- Weighted average across grades for a district in a given year
SELECT
  SUM(pct_met_and_above * students_tested) / SUM(students_tested) AS weighted_pct
FROM read_parquet('s3://stuff/sbac_data.parquet')
WHERE type_id = 6
  AND district_code = '...'
  AND year = 2024
  AND test_id = 1
  AND student_group_id = 1
  AND pct_met_and_above IS NOT NULL;
```

## DuckDB connection (R2)

```sql
INSTALL httpfs; LOAD httpfs;
SET s3_endpoint = '0ed4ae7d5070c68de6ce908a38ccc968.r2.cloudflarestorage.com';
SET s3_region = 'auto';
-- provide credentials or use a public URL if bucket is public
```
