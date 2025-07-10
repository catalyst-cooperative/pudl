{% test expect_columns_not_all_null(
    model,
    exclude_columns=[],
    row_conditions={}
) %}

-- DESCRIBE implementations vary; this assumes DuckDB's
{% set get_columns_query %}
    SELECT column_name
    FROM (DESCRIBE {{ model }})
    WHERE column_name NOT IN ('{{ exclude_columns | join("', '") }}')
{% endset %}

{% if execute %}
    {% set columns_result = run_query(get_columns_query) %}
    {% set column_names = columns_result.columns[0].values() %}
{% else %}
    {% set column_names = [] %}
{% endif %}

{% set checks = [] %}
{% for column_name in column_names %}
    {% if column_name in row_conditions %}
        {% set row_condition = row_conditions[column_name] %}
        {% set check %}
            SELECT
                '{{ model.name }}' as table_name,
                '{{ column_name }}' as failing_column,
                'Conditional check failed: {{ row_condition }}' as failure_reason,
                '{{ row_condition | replace("'", "''") }}' as row_condition,
                COUNT(*) as total_rows_matching_condition,
                COUNT({{ column_name }}) as non_null_count
            FROM {{ model }}
            WHERE {{ row_condition }}
            HAVING COUNT(*) > 0 AND COUNT({{ column_name }}) = 0
        {% endset %}
    {% else %}
        {% set check %}
            SELECT
                '{{ model.name }}' as table_name,
                '{{ column_name }}' as failing_column,
                'Column is entirely NULL' as failure_reason,
                'N/A (entire table)' as row_condition,
                COUNT(*) as total_rows_matching_condition,
                COUNT({{ column_name }}) as non_null_count
            FROM {{ model }}
            HAVING COUNT({{ column_name }}) = 0
        {% endset %}
    {% endif %}
    {% do checks.append(check) %}
{% endfor %}

WITH column_null_checks AS (
{% if checks %}
    {{ checks | join('\nUNION ALL\n') }}
{% else %}
    SELECT NULL as table_name, NULL as failing_column, NULL as failure_reason, NULL as row_condition WHERE FALSE
{% endif %}
)

SELECT * FROM column_null_checks

{% endtest %}
