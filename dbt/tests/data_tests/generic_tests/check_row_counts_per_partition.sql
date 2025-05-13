{% test check_row_counts_per_partition(model, table_name, partition_column) %}

{% if target.name == "etl-fast" %}
SELECT NULL WHERE FALSE -- This always returns zero rows, so the test will always pass
{% else %}
WITH
    expected AS (
        SELECT table_name, CAST(partition as VARCHAR) as partition, row_count as expected_count
        FROM {{ ref("etl_full_row_counts") }}
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
SELECT expected.partition, expected.expected_count, observed.observed_count
FROM expected
INNER JOIN observed ON expected.partition=observed.partition
WHERE expected.expected_count != observed.observed_count
{% endif %}

{% endtest %}
