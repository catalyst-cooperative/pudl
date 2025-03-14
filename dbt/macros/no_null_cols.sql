{% test no_null_cols(model, filter_condition=None, filtered_columns=[]) %}

WITH column_checks AS (
    {% set columns = dbt_utils.get_columns_in_relation(model) %}

    {% for column in columns %}
        {% set column_name = column.name %}

        SELECT
            '{{ column_name }}' AS column_name,
            COUNT(*) AS relevant_rows,  -- Count rows in scope (filtered or full)
            {% if column_name in filtered_columns and filter_condition %}
                COUNT(CASE WHEN {{ filter_condition }} THEN {{ column_name }} END) AS non_null_rows
            {% else %}
                COUNT({{ column_name }}) AS non_null_rows
            {% endif %}
        FROM {{ model }}

        {% if not loop.last %} UNION ALL {% endif %}

    {% endfor %}
)

SELECT STRING_AGG(column_name, ', ') AS null_columns
FROM column_checks
WHERE relevant_rows > 0  -- Ignore columns where all rows were filtered out
  AND non_null_rows = 0   -- Fail only if the column is entirely NULL in relevant rows

{% endtest %}
