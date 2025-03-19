{% test no_null_cols(model, partial_columns=[]) %}

WITH column_checks AS (
    {% set columns = adapter.get_columns_in_relation(model) %}

    {% for column in columns %}
        {% set column_name = column.name %}
        {% set condition_sql = "1=1" %}  -- Default: test all rows

        {# Check if this column has a specific condition #}
        {% for partial_col in partial_columns %}
            {% if partial_col.get('column_name') == column_name %}
                {% set condition_sql = partial_col.get('expect_at_least_one_value_when') %}
            {% endif %}
        {% endfor %}
        
        SELECT 
            '{{ column_name }}' AS column_name,
            COUNT(CASE WHEN {{ condition_sql }} THEN 1 END) AS relevant_rows,  -- Count rows in scope (filtered or full)
            COUNT(CASE WHEN {{ condition_sql }} THEN {{ column_name }} END) AS non_null_rows
        FROM {{ model }}

        {% if not loop.last %} UNION ALL {% endif %}

    {% endfor %}
)

SELECT STRING_AGG(column_name, ', ') AS null_columns
FROM column_checks
WHERE relevant_rows > 0  -- Ignore columns where all rows were filtered out
  AND non_null_rows = 0   -- Fail only if the column is entirely NULL in relevant rows

{% endtest %}
