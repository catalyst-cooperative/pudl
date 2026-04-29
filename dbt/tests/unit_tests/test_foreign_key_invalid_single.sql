WITH parent_table AS (
    SELECT * FROM (VALUES
        ('p1', 'alpha'),
        ('p2', 'beta')
    ) AS t(parent_key, parent_name)
),
child_table AS (
    SELECT * FROM (VALUES
        (1, 'p9', 'orphan child')
    ) AS t(child_id, child_parent_key, child_name)
),
observed_failures AS (
    {{ test_foreign_key(
        'child_table',
        ['child_parent_key'],
        'parent_table',
        ['parent_key'],
    ) }}
),
expected_failures AS (
    SELECT * FROM (VALUES
        (1, 'p9', 'orphan child', 'missing_parent_key')
    ) AS t(child_id, child_parent_key, child_name, failure_type)
),
missing_failures AS (
    SELECT expected.*
    FROM expected_failures AS expected
    ANTI JOIN observed_failures AS observed
    USING (child_id, child_parent_key, child_name, failure_type)
),
unexpected_failures AS (
    SELECT observed.*
    FROM observed_failures AS observed
    ANTI JOIN expected_failures AS expected
    USING (child_id, child_parent_key, child_name, failure_type)
)

SELECT *
FROM missing_failures
UNION ALL
SELECT *
FROM unexpected_failures
