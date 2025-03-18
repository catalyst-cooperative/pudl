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
    SELECT * FROM (VALUES
        (0.00, 10),  -- Minimum value (no interpolation)
        (0.25, 25),  -- Requires interpolation
        (0.50, 35),  -- Requires interpolation
        (0.75, 42.5),-- Requires interpolation
        (1.00, 50)   -- Maximum value (no interpolation)
    ) AS t(quantile, expected_value)
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
