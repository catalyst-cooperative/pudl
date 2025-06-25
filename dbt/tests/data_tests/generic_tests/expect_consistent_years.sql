-- Generic test: expect_consistent_years
--
-- This test validates that two temporal columns in a model have consistent years
-- for the vast majority of records. It's designed for tables with both a high-frequency
-- datetime column (hourly or more frequent) and a separate date/year column that
-- describes the reporting period.
--
-- Use Case:
-- Due to timezone adjustments, a small fraction of records may have different years
-- between these columns (e.g., UTC datetime shows Jan 1 while report_date shows Dec 31
-- of the previous year). This test ensures such mismatches stay within acceptable limits.
--
-- Test Logic:
-- 1. Extract years from both temporal columns
-- 2. Count total records and records with mismatched years
-- 3. Calculate the mismatch ratio
-- 4. Fail if the ratio exceeds the specified threshold
--
-- Parameters:
-- - model: The model to test
-- - datetime_column: High-frequency datetime column (e.g., 'datetime_utc')
-- - date_column: Reporting period date/year column (e.g., 'report_date')
-- - max_mismatch_ratio: Maximum allowed fraction of mismatched records (default: 0.001 = 0.1%)

{% test expect_consistent_years(
    model,
    datetime_column,
    date_column,
    max_mismatch_ratio=0.001
) %}

-- Calculate year mismatch statistics
WITH year_comparison AS (
    SELECT
        EXTRACT(YEAR FROM {{ datetime_column }}) as datetime_year,
        EXTRACT(YEAR FROM {{ date_column }}) as date_year,
        CASE
            WHEN EXTRACT(YEAR FROM {{ datetime_column }}) != EXTRACT(YEAR FROM {{ date_column }})
            THEN 1
            ELSE 0
        END as is_mismatch
    FROM {{ model }}
    WHERE {{ datetime_column }} IS NOT NULL
      AND {{ date_column }} IS NOT NULL
),

-- Aggregate mismatch statistics
mismatch_stats AS (
    SELECT
        COUNT(*) as total_records,
        SUM(is_mismatch) as mismatched_records,
        CASE
            WHEN COUNT(*) > 0
            THEN CAST(SUM(is_mismatch) AS FLOAT) / COUNT(*)
            ELSE 0
        END as mismatch_ratio
    FROM year_comparison
)

-- Test fails when mismatch ratio exceeds threshold (returns rows on failure)
SELECT
    total_records,
    mismatched_records,
    mismatch_ratio,
    {{ max_mismatch_ratio }} as max_allowed_ratio,
    'Found ' || ROUND(mismatch_ratio * 100, 5) || '% (' || mismatched_records ||
    ' of ' || total_records || ') mismatched years between ' ||
    '{{ datetime_column }}' || ' and ' || '{{ date_column }}' ||
    ', exceeds threshold of ' || ROUND({{ max_mismatch_ratio }} * 100, 5) || '%'
    as failure_reason
FROM mismatch_stats
WHERE mismatch_ratio > {{ max_mismatch_ratio }}

{% endtest %}
