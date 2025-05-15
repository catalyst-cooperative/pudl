WITH test_row_counts AS (
    SELECT * FROM (VALUES
        ('test_table', '', 1),
    ) AS t(table_name, partition, row_count)
),

test_table AS (
    SELECT * FROM (VALUES
        ('x'),
    ) AS t(dummy_col)
),

expected_mismatch_counts as (
    SELECT * FROM (VALUES
        ('test_table', 0),
    ) AS t(table_name, num_mismatches)
),

observed_mismatches AS (
    {{
        row_counts_per_partition('test_table', 'test_table', none, force_row_counts_table='test_row_counts')
    }}
),

result_comparison AS (
    SELECT (SELECT COUNT(*)
    FROM observed_mismatches) as observed_mismatch_count,
    num_mismatches AS expected_mismatch_count,
    FROM expected_mismatch_counts
)

SELECT *
FROM result_comparison
WHERE observed_mismatch_count != expected_mismatch_count
