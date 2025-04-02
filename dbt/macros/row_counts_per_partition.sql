{% macro row_counts_per_partition(model, table_name, partition_column, force_row_counts_table=none) %}
{% set row_counts_table = force_row_counts_table if force_row_counts_table is not none
    else ref("etl_fast_row_counts") if target.name == "etl-fast"
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
        {% if partition_column == "report_year" %}
        SELECT COALESCE(CAST({{ partition_column }} as VARCHAR), '') as partition, COUNT(*) as observed_count
        FROM {{ model }}
        GROUP BY {{ partition_column }}
        {% elif partition_column in ["report_date", "datetime_utc"] %}
        SELECT COALESCE(CAST(YEAR({{ partition_column }}) as VARCHAR), '') as partition, COUNT(*) as observed_count
        FROM {{ model }}
        GROUP BY YEAR({{ partition_column }})
        {% else %}
        SELECT '' as partition, COUNT(*) as observed_count
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
