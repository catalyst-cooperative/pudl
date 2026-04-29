WITH parent_table AS (
    SELECT * FROM (VALUES
        ('l1', 'r1', 'alpha'),
        ('l2', 'r2', 'beta')
    ) AS t(parent_left_key, parent_right_key, parent_name)
),
child_table AS (
    SELECT * FROM (VALUES
        (1, 'l1', 'r1', 'alpha child'),
        (2, NULL, NULL, 'unassigned child'),
        (3, 'l2', NULL, 'partial child')
    ) AS t(child_id, child_left_key, child_right_key, child_name)
),
observed_failures AS (
    {{ test_foreign_key(
        'child_table',
        ['child_left_key', 'child_right_key'],
        'parent_table',
        ['parent_left_key', 'parent_right_key'],
    ) }}
)

SELECT *
FROM observed_failures
