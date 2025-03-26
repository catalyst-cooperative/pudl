{% test check_row_counts_per_partition(model, table_name, partition_column) %}

WITH
    expected AS (
        SELECT table_name, CAST(partition as VARCHAR) as partition, row_count as expected_count
        {% if target.name == "etl-fast" %}
        FROM {{ ref("etl_fast_row_counts") }}
        {% else %}
        FROM {{ ref("etl_full_row_counts") }}
        {% endif %}
        WHERE table_name = '{{ table_name }}'
    ),
    observed AS (
        {% if partition_column == "report_year" %}
        SELECT CAST({{ partition_column }} as VARCHAR) as partition, COUNT(*) as observed_count
        FROM {{ model }}
        GROUP BY {{ partition_column }}
        {% elif partition_column in ["report_date", "datetime_utc"] %}
        SELECT CAST(YEAR({{ partition_column }}) as VARCHAR) as partition, COUNT(*) as observed_count
        FROM {{ model }}
        GROUP BY YEAR({{ partition_column }})
        {% else %}
        SELECT '' as partition, COUNT(*) as observed_count
        FROM {{ model }}
        {% endif %}
    )
SELECT expected.partition as expected_partition, observed.partition as observed_partition, expected.expected_count, observed.observed_count
FROM expected
FULL OUTER JOIN observed ON expected.partition=observed.partition
WHERE expected.expected_count != observed.observed_count
  OR expected.expected_count IS NULL
  OR observed.observed_count IS NULL

{% endtest %}
