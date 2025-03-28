WITH test_row_counts AS (
    SELECT * FROM (VALUES
        ('test_table', 2022, 2),
    ) AS t(table_name, partition, row_count)
),

test_table AS (
    SELECT * FROM (VALUES
        (2022, 'x'),
    ) AS t(report_year, dummy_col)
),

expected_mismatch_counts as (
    SELECT * FROM (VALUES
        ('test_table', 1),
    ) AS t(table_name, num_mismatches)
),

result_comparison AS (
    SELECT (SELECT COUNT(*)
    FROM ({{
        row_counts_per_partition('test_table', 'test_table', 'report_year', force_row_counts_table='test_row_counts')
    }})) as observed_mismatch_count,
    num_mismatches AS expected_mismatch_count,
    FROM expected_mismatch_counts
)

SELECT *
FROM result_comparison
WHERE observed_mismatch_count != expected_mismatch_count
