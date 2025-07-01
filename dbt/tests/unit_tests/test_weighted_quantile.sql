{{ config(tags = ['unit','weighted_quantile']) }}

WITH test_data AS (
    -- Mock dataset for testing
    SELECT * FROM (VALUES
        ( 200, NULL, 0),
        (NULL,  200, 0),
        (  10,    2, 0),
        (  20,    3, 0),
        (  30,    5, 0),
        (  40,   10, 1),
        (  50,   10, 1),
        ( 100, NULL, 1),
        (NULL,  100, 1)
    ) AS t(column_name, weight_col, selector)
),

expected_results_all AS (
    SELECT * FROM (VALUES
        (0.00, 10),  -- Minimum value (no interpolation)
        (0.25, 30),  -- (no interpolation)
        (0.50, 40),  -- (no interpolation)
        (0.75, 47.5),-- Requires interpolation
        (1.00, 50)   -- Maximum value (no interpolation)
    ) AS t(quantile, expected_value)
),

expected_results_zero AS (
    SELECT * FROM (VALUES
        (0.00, 10),   -- Minimum value (no interpolation)
        (0.25, 16),   -- Requires interpolation
        (0.50, 23.75),-- Requires interpolation
        (0.75, 30),   -- (no interpolation)
        (1.00, 30)    -- Maximum value (no interpolation)
    ) AS t(quantile, expected_value)
),

actual_results AS (
    -- Call the macro for different quantiles
    SELECT
        'all' as row_condition,
        e.quantile,
        (select * from ({{ weighted_quantile("test_data", "column_name", "weight_col", "e.quantile") }})) AS computed_value,
        e.expected_value
    FROM expected_results_all e

    UNION ALL

    SELECT
        'zero' as row_condition,
        e.quantile,
        (select * from ({{ weighted_quantile("test_data", "column_name", "weight_col", "e.quantile", "selector = 0") }})) AS computed_value,
        e.expected_value
    FROM expected_results_zero e
)

-- The test fails if computed_value ≠ expected_value
SELECT *
FROM actual_results
WHERE computed_value != expected_value
