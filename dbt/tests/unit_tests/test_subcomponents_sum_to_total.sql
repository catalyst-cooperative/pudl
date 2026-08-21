{{ config(tags = ['unit', 'subcomponents_sum_to_total']) }}

WITH test_costs AS (
    -- Mock dataset with a two-column categorical hierarchy (grp, cat) where the
    -- cat value "total" repeats across groups, so cross-group calculations can
    -- only be expressed with (grp, cat) tuples.
    SELECT * FROM (VALUES
        -- entity_1: internally consistent everywhere
        ('entity_1', 'ops',   'a',     10.0),
        ('entity_1', 'ops',   'b',     20.0),
        ('entity_1', 'ops',   'total', 30.0),
        ('entity_1', 'maint', 'a',      5.0),
        ('entity_1', 'maint', 'total',  5.0),
        ('entity_1', 'adj',   'total',  2.0),
        ('entity_1', 'grand', 'total', 33.0), -- ops.total + maint.total - adj.total
        -- entity_2: ops subcomponents consistent, grand total inconsistent
        ('entity_2', 'ops',   'a',     10.0),
        ('entity_2', 'ops',   'b',     20.0),
        ('entity_2', 'ops',   'total', 30.0),
        ('entity_2', 'maint', 'a',      7.0),
        ('entity_2', 'maint', 'total',  7.0),
        ('entity_2', 'adj',   'total',  0.0),
        ('entity_2', 'grand', 'total', 99.0), -- should be 37
        -- entity_3: ops subcomponents inconsistent, but only just outside tolerance
        ('entity_3', 'ops',   'a',     10.0),
        ('entity_3', 'ops',   'b',     20.0),
        ('entity_3', 'ops',   'total', 31.5), -- should be 30; diff 1.5 > tolerance 1
        -- entity_4: ops subcomponents inconsistent, but within tolerance
        ('entity_4', 'ops',   'a',     10.0),
        ('entity_4', 'ops',   'b',     20.0),
        ('entity_4', 'ops',   'total', 30.5), -- diff 0.5 <= tolerance 1
    ) AS t(entity_id, grp, cat, val)
),

expected_mismatch_counts AS (
    SELECT * FROM (VALUES
        -- Single-column mode: sum cat IN (a, b) vs cat = total among ops rows.
        -- Only entity_3 is off by more than the tolerance of 1.
        ('single_column_subcomponents', 1),
        -- Single-column "everything except the total" mode over ops rows:
        -- same expectation as the explicit list above.
        ('single_column_all_but_total', 1),
        -- Tuple mode: (ops, total) + (maint, total) - (adj, total) vs
        -- (grand, total). Only entity_2's grand total is inconsistent;
        -- entity_3 and entity_4 have no grand total so they are not counted.
        ('tuple_cross_group_totals', 1)
    ) AS t(check_name, num_mismatches)
),

observed_mismatch_counts AS (
    SELECT
        'single_column_subcomponents' AS check_name,
        (SELECT COUNT(*) FROM ({{
            subcomponents_sum_to_total_check(
                'test_costs',
                group_by_columns=['entity_id'],
                categorical_column='cat',
                value_column='val',
                subcomponents_list=['a', 'b'],
                total_label='total',
                tolerance=1,
                row_condition="grp = 'ops'"
            )
        }})) AS num_mismatches

    UNION ALL

    SELECT
        'single_column_all_but_total' AS check_name,
        (SELECT COUNT(*) FROM ({{
            subcomponents_sum_to_total_check(
                'test_costs',
                group_by_columns=['entity_id'],
                categorical_column='cat',
                value_column='val',
                total_label='total',
                tolerance=1,
                row_condition="grp = 'ops'"
            )
        }})) AS num_mismatches

    UNION ALL

    SELECT
        'tuple_cross_group_totals' AS check_name,
        (SELECT COUNT(*) FROM ({{
            subcomponents_sum_to_total_check(
                'test_costs',
                group_by_columns=['entity_id'],
                categorical_column=['grp', 'cat'],
                value_column='val',
                subcomponents_list=[['ops', 'total'], ['maint', 'total']],
                negative_subcomponents_list=[['adj', 'total']],
                total_label=['grand', 'total'],
                tolerance=1
            )
        }})) AS num_mismatches
)

-- The test fails if any check has an unexpected number of mismatching records
SELECT
    expected.check_name,
    observed.num_mismatches AS observed_mismatches,
    expected.num_mismatches AS expected_mismatches
FROM expected_mismatch_counts AS expected
JOIN observed_mismatch_counts AS observed USING (check_name)
WHERE observed.num_mismatches != expected.num_mismatches
