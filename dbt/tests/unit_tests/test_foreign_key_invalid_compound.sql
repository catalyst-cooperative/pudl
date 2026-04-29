WITH parent_table AS (
    SELECT * FROM (VALUES
        ('l1', 'r1', 'alpha'),
        ('l2', 'r2', 'beta')
    ) AS t(parent_left_key, parent_right_key, parent_name)
),
child_table AS (
    SELECT * FROM (VALUES
        (1, 'l1', 'r9', 'broken child')
    ) AS t(child_id, child_left_key, child_right_key, child_name)
),
observed_failures AS (
    {{ test_foreign_key(
        'child_table',
        ['child_left_key', 'child_right_key'],
        'parent_table',
        ['parent_left_key', 'parent_right_key'],
    ) }}
),
expected_failures AS (
    SELECT * FROM (VALUES
        (1, 'l1', 'r9', 'broken child', 'missing_parent_key')
    ) AS t(
        child_id,
        child_left_key,
        child_right_key,
        child_name,
        failure_type
    )
),
missing_failures AS (
    SELECT expected.*
    FROM expected_failures AS expected
    ANTI JOIN observed_failures AS observed
    USING (
        child_id,
        child_left_key,
        child_right_key,
        child_name,
        failure_type
    )
),
unexpected_failures AS (
    SELECT observed.*
    FROM observed_failures AS observed
    ANTI JOIN expected_failures AS expected
    USING (
        child_id,
        child_left_key,
        child_right_key,
        child_name,
        failure_type
    )
)

SELECT *
FROM missing_failures
UNION ALL
SELECT *
FROM unexpected_failures
