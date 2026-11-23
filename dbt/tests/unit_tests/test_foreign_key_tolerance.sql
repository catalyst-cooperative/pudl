-- Each case should (or should not) return failure rows. The final SELECT reports any
-- case that disagrees with its expectation, so it returns no rows if the test works.
WITH parent_table AS (
    SELECT * FROM (VALUES
        ('q1', 'C1', TRUE),
        ('q1', 'C2', FALSE)
    ) AS t(parent_quarter, parent_id, parent_flag)
),
child_table AS (
    SELECT * FROM (VALUES
        ('q1', 'C1', 1),
        ('q1', 'C1', 2),
        ('q1', 'C2', 3),
        ('q1', 'C9', 4),
        ('q1', NULL, 5)
    ) AS t(child_quarter, child_id, child_n)
),
cases AS (
    -- Strict by default: 1 of 4 non-null rows (NULL keys are skipped) is missing.
    SELECT 'strict_fail' AS case_name, 1 AS expected_rows, COUNT(*) AS n_rows
    FROM ({{ test_foreign_key(
        'child_table', ['child_quarter', 'child_id'],
        'parent_table', ['parent_quarter', 'parent_id']
    ) }})
    UNION ALL
    -- 1 of 4 rows is missing (0.25): fails at 0.2, passes at 0.3.
    SELECT 'tolerance_fail', 1, COUNT(*)
    FROM ({{ test_foreign_key(
        'child_table', ['child_quarter', 'child_id'],
        'parent_table', ['parent_quarter', 'parent_id'], max_fraction=0.2
    ) }})
    UNION ALL
    SELECT 'tolerance_pass', 0, COUNT(*)
    FROM ({{ test_foreign_key(
        'child_table', ['child_quarter', 'child_id'],
        'parent_table', ['parent_quarter', 'parent_id'], max_fraction=0.3
    ) }})
    UNION ALL
    -- Restricting the parent to flagged rows makes C2 missing too (2 of 4), and the
    -- output is capped by sample_size.
    SELECT 'parent_condition_sample', 1, COUNT(*)
    FROM ({{ test_foreign_key(
        'child_table', ['child_quarter', 'child_id'],
        'parent_table', ['parent_quarter', 'parent_id'],
        max_fraction=0.4, parent_row_condition='parent_flag', sample_size=1
    ) }})
    UNION ALL
    -- Restricting the child to rows with child_n <= 3 leaves nothing missing.
    SELECT 'child_condition_pass', 0, COUNT(*)
    FROM ({{ test_foreign_key(
        'child_table', ['child_quarter', 'child_id'],
        'parent_table', ['parent_quarter', 'parent_id'], row_condition='child_n <= 3'
    ) }})
    UNION ALL
    -- A child condition that selects no rows at all passes.
    SELECT 'no_rows_pass', 0, COUNT(*)
    FROM ({{ test_foreign_key(
        'child_table', ['child_quarter', 'child_id'],
        'parent_table', ['parent_quarter', 'parent_id'], row_condition='child_n > 100'
    ) }})
),
-- The reported failure rate and counts should describe all failures, not the sample.
rate_check AS (
    SELECT 'reports_rate' AS case_name, 1 AS expected_rows,
        COUNT(*) FILTER (
            WHERE n_failures = 2 AND n_checked = 4 AND failure_rate = 0.5
        ) AS n_rows
    FROM ({{ test_foreign_key(
        'child_table', ['child_quarter', 'child_id'],
        'parent_table', ['parent_quarter', 'parent_id'],
        parent_row_condition='parent_flag', sample_size=1
    ) }})
)

SELECT * FROM cases WHERE n_rows != expected_rows
UNION ALL
SELECT * FROM rate_check WHERE n_rows != expected_rows
