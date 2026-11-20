-- Each case below should (or should not) produce a failure row; the final SELECT
-- reports any case that disagrees with its expectation, and returns no rows if the
-- test behaves as intended.
WITH parent_table AS (
    SELECT * FROM (VALUES
        ('2024q1', 'C1', TRUE),
        ('2024q1', 'C2', FALSE)
    ) AS t(parent_quarter, parent_id, parent_flag)
),
child_table AS (
    SELECT * FROM (VALUES
        ('2024q1', 'C1', 1),
        ('2024q1', 'C1', 2),
        ('2024q1', 'C2', 3),
        ('2024q1', 'C9', 4),
        ('2024q1', NULL, 5)
    ) AS t(child_quarter, child_id, child_n)
),
cases AS (
    -- 1 of 4 non-null rows is missing (0.25): fails at 0.2, passes at 0.3.
    SELECT 'rows_fail' AS case_name, TRUE AS expect_failure, COUNT(*) AS n_failures
    FROM ({{ test_expect_fraction_missing_foreign_key(
        'child_table', ['child_quarter', 'child_id'],
        'parent_table', ['parent_quarter', 'parent_id'], max_fraction=0.2) }})
    UNION ALL
    SELECT 'rows_pass', FALSE, COUNT(*)
    FROM ({{ test_expect_fraction_missing_foreign_key(
        'child_table', ['child_quarter', 'child_id'],
        'parent_table', ['parent_quarter', 'parent_id'], max_fraction=0.3) }})
    UNION ALL
    -- 1 of 3 distinct keys is missing (0.33): fails at 0.3, passes at 0.4.
    SELECT 'distinct_fail', TRUE, COUNT(*)
    FROM ({{ test_expect_fraction_missing_foreign_key(
        'child_table', ['child_quarter', 'child_id'],
        'parent_table', ['parent_quarter', 'parent_id'],
        max_fraction=0.3, distinct_keys=true) }})
    UNION ALL
    SELECT 'distinct_pass', FALSE, COUNT(*)
    FROM ({{ test_expect_fraction_missing_foreign_key(
        'child_table', ['child_quarter', 'child_id'],
        'parent_table', ['parent_quarter', 'parent_id'],
        max_fraction=0.4, distinct_keys=true) }})
    UNION ALL
    -- Restricting the parent to flagged rows makes C2 missing too (2 of 4 = 0.5).
    SELECT 'parent_condition_fail', TRUE, COUNT(*)
    FROM ({{ test_expect_fraction_missing_foreign_key(
        'child_table', ['child_quarter', 'child_id'],
        'parent_table', ['parent_quarter', 'parent_id'],
        max_fraction=0.4, parent_row_condition='parent_flag') }})
    UNION ALL
    -- Restricting the child to rows with child_n <= 3 leaves nothing missing.
    SELECT 'child_condition_pass', FALSE, COUNT(*)
    FROM ({{ test_expect_fraction_missing_foreign_key(
        'child_table', ['child_quarter', 'child_id'],
        'parent_table', ['parent_quarter', 'parent_id'],
        max_fraction=0.0, row_condition='child_n <= 3') }})
)

SELECT * FROM cases
WHERE (expect_failure AND n_failures = 0)
    OR (NOT expect_failure AND n_failures > 0)
