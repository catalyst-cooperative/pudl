WITH test_data AS (
    -- Mock dataset for testing
    SELECT * FROM (VALUES 
        (10, 2),
        (20, 3),
        (30, 5),
        (40, 10),
        (50, 10)
    ) AS t(column_name, weight_col)
),

expected_results AS (
    -- Expected output for different quantiles
    SELECT 0.25 AS quantile, 20 AS expected_value
    UNION ALL
    SELECT 0.50 AS quantile, 40 AS expected_value
    UNION ALL
    SELECT 0.75 AS quantile, 50 AS expected_value
),

actual_results AS (
    -- Call the macro for different quantiles
    SELECT 
        e.quantile,
        (select * from ({{ weighted_quantile("test_data", "column_name", "weight_col", "e.quantile") }})) AS computed_value,
        e.expected_value
    FROM expected_results e
)

-- The test fails if computed_value ≠ expected_value
SELECT *
FROM actual_results
WHERE computed_value != expected_value
