{% macro row_counts_per_partition(model, table_name, partition_expr, force_row_counts_table=none) %}
{% set row_counts_table = force_row_counts_table if force_row_counts_table is not none
    else ref("etl_full_row_counts") if target.name == "etl-full"
    else force_row_counts_table
%}

WITH
    expected AS (
        SELECT table_name, COALESCE(CAST(partition as VARCHAR), '') as partition, row_count as expected_count
        FROM {{ row_counts_table }}
        WHERE table_name = '{{ table_name }}'
    ),
    observed AS (
        {% if partition_expr %}
        SELECT
            COALESCE(CAST({{ partition_expr }} AS VARCHAR), '') AS partition,
            COUNT(*) AS observed_count
        FROM {{ model }}
        GROUP BY {{ partition_expr }}
        {% else %}
        SELECT
            '' AS partition,
            COUNT(*) AS observed_count
        FROM {{ model }}
        {% endif %}
    )
SELECT
    '{{ table_name }}' as table_name,
    expected.partition as expected_partition,
    observed.partition as observed_partition,
    expected_count,
    observed_count,
    (observed_count - expected_count) / expected_count as diff_relative_to_expected,
FROM expected
FULL OUTER JOIN observed ON expected.partition=observed.partition
WHERE expected_count != observed_count
    OR expected_count IS NULL
    OR observed_count IS NULL

{% endmacro %}
