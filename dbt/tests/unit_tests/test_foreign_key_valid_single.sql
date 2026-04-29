WITH parent_table AS (
    SELECT * FROM (VALUES
        ('p1', 'alpha'),
        ('p2', 'beta')
    ) AS t(parent_key, parent_name)
),
child_table AS (
    SELECT * FROM (VALUES
        (1, 'p1', 'alpha child'),
        (2, 'p2', 'beta child')
    ) AS t(child_id, child_parent_key, child_name)
),
observed_failures AS (
    {{ test_foreign_key(
        'child_table',
        ['child_parent_key'],
        'parent_table',
        ['parent_key'],
    ) }}
)

SELECT *
FROM observed_failures
