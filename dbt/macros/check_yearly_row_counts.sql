{% test check_yearly_row_counts(model, table_name, partition_column) %}

WITH
    expected AS (
        SELECT table_name, partition, row_count as expected_count
        FROM {{ ref("row_counts") }} WHERE table_name = '{{ table_name }}'
    ),
    observed AS (
        SELECT {{ partition_column }} as partition, COUNT(*) as observed_count
        FROM {{ model }}
        GROUP BY {{ partition_column }}
    )
SELECT expected.partition, expected.expected_count, observed.observed_count
FROM expected
INNER JOIN observed ON expected.partition=observed.partition
WHERE expected.expected_count != observed.observed_count

{% endtest %}
