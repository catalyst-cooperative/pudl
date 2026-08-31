WITH model_table AS (
    -- year 2020: A and B are matched, C is entirely missing from other_table -> 1/3 unmatched
    -- year 2021: D is matched, E is present in other_table but with a null match column -> 1/2 unmatched
    SELECT * FROM (VALUES
        (2020, 'A'),
        (2020, 'B'),
        (2020, 'C'),
        (2021, 'D'),
        (2021, 'E')
    ) AS t(model_year, model_key)
),
other_table AS (
    SELECT * FROM (VALUES
        ('A', 'gen1'),
        ('B', 'gen2'),
        ('D', 'gen3'),
        ('E', NULL)
    ) AS t(other_key, other_match)
),
observed_failures AS (
    {{ test_expect_grouped_unmatched_rate_to_be_below(
        'model_table',
        'model_year',
        'model_key',
        'other_table',
        'other_key',
        'other_match',
        0.4,
    ) }}
),
expected_failures AS (
    -- Only 2021 (0.5 unmatched) exceeds the 0.4 threshold; 2020 (0.333 unmatched) does not.
    SELECT * FROM (VALUES
        (2021, 2, 1, 1, 0.5)
    ) AS t(
        group_value,
        total_keys,
        matched_keys,
        unmatched_keys,
        unmatched_ratio
    )
),
missing_failures AS (
    SELECT expected.*
    FROM expected_failures AS expected
    ANTI JOIN observed_failures AS observed
    USING (group_value, total_keys, matched_keys, unmatched_keys, unmatched_ratio)
),
unexpected_failures AS (
    SELECT observed.*
    FROM observed_failures AS observed
    ANTI JOIN expected_failures AS expected
    USING (group_value, total_keys, matched_keys, unmatched_keys, unmatched_ratio)
)

SELECT *
FROM missing_failures
UNION ALL
SELECT *
FROM unexpected_failures
