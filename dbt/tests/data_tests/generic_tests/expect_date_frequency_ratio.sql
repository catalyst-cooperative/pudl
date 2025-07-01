-- Generic test: expect_date_frequency_ratio
--
-- This test validates that two models have the expected ratio of distinct dates
-- for overlapping years. It's commonly used to verify that monthly and annual
-- data have the correct 12:1 relationship, but works for any frequency ratio.
--
-- Test Logic:
-- 1. Extract years and count distinct dates per year for both models
-- 2. Join on overlapping years only (ignores years present in just one model)
-- 3. Sum the total distinct dates across all overlapping years
-- 4. Verify that model_total = compare_total * multiplier
--
-- Parameters:
-- - model: The model to test (typically higher frequency, e.g. monthly)
-- - compare_model: The reference model to compare against (typically lower frequency, e.g. annual)
-- - multiplier: Expected ratio (e.g. 12 for monthly:annual comparison)
-- - date_column: Date column name (default: 'report_date')
-- - model_has_data_maturity: Tell the test that the data_maturity column exists in the model (default: true). If this column is available, it will be used to filter out incomplete years of data.
-- - compare_model_has_data_maturity: Tell the test that the data_maturity column exists in the model (default: true). If this column is available, it will be used to filter out incomplete years of data.
--
-- Data Filtering:
-- - Excludes NULL dates
-- - Excludes provisional records (data_maturity = 'incremental_ytd' or 'monthly_update')
--   when the respective has_data_maturity parameter is true

{% test expect_date_frequency_ratio(
    model,
    compare_model,
    multiplier,
    date_column='report_date',
    model_has_data_maturity=true,
    compare_model_has_data_maturity=true
) %}

-- Count distinct dates per year for the main model (typically higher frequency)
WITH model_years AS (
    SELECT
        EXTRACT(YEAR FROM {{ date_column }}) as year,
        COUNT(DISTINCT {{ date_column }}) as date_count
    FROM {{ model }}
    WHERE {{ date_column }} IS NOT NULL
    {% if model_has_data_maturity %}
        -- Exclude provisional data if data_maturity column exists
        AND (data_maturity NOT IN ('incremental_ytd', 'monthly_update') OR data_maturity IS NULL)
    {% endif %}
    GROUP BY EXTRACT(YEAR FROM {{ date_column }})
),

-- Count distinct dates per year for the comparison model (typically lower frequency)
compare_years AS (
    SELECT
        EXTRACT(YEAR FROM {{ date_column }}) as year,
        COUNT(DISTINCT {{ date_column }}) as date_count
    FROM {{ compare_model }}
    WHERE {{ date_column }} IS NOT NULL
    {% if compare_model_has_data_maturity %}
        -- Exclude provisional data if data_maturity column exists
        AND (data_maturity NOT IN ('incremental_ytd', 'monthly_update') OR data_maturity IS NULL)
    {% endif %}
    GROUP BY EXTRACT(YEAR FROM {{ date_column }})
),

-- Sum totals across all overlapping years (INNER JOIN ensures only shared years)
totals AS (
    SELECT
        COALESCE(SUM(m.date_count), 0) as model_total,
        COALESCE(SUM(c.date_count), 0) as compare_total
    FROM model_years m
    INNER JOIN compare_years c ON m.year = c.year
)

-- Test fails when totals don't match expected ratio (returns rows on failure)
-- Expected: model_total = compare_total * multiplier
SELECT
    model_total,
    compare_total,
    {{ multiplier }} as expected_multiplier,
    'Expected ' || (compare_total * {{ multiplier }}) ||
    ' = ' || compare_total || ' * ' || {{ multiplier }} ||
    ', but found ' || model_total
    as failure_reason
FROM totals
WHERE model_total != compare_total * {{ multiplier }}

{% endtest %}
