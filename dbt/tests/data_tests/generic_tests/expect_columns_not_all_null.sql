{% test expect_columns_not_all_null(
    model,
    exclude_columns=[],
    row_conditions={},
    ignore_eia860m_nulls=false
) %}

-- DESCRIBE implementations vary; this assumes DuckDB's
{% set get_columns_query %}
    SELECT column_name
    FROM (DESCRIBE {{ model }})
    WHERE column_name NOT IN ('{{ exclude_columns | join("', '") }}')
{% endset %}

{% if ignore_eia860m_nulls %}
    {% set get_last_eia860_year_query %}
        SELECT EXTRACT(year FROM MAX(report_date)) as last_eia860_year
        FROM {{ source('pudl', 'core_eia860__scd_ownership') }}
    {% endset %}

    {% if execute %}
        {% set last_eia860_year_result = run_query(get_last_eia860_year_query) %}
        {% set last_eia860_year = last_eia860_year_result.columns[0].values()[0] %}
    {% else %}
        {% set last_eia860_year = none %}
    {% endif %}
{% endif %}

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
        {% if ignore_eia860m_nulls and last_eia860_year %}
            {% set combined_condition = "(" + row_condition + ") AND EXTRACT(year FROM report_date) <= " + last_eia860_year|string %}
            {% set failure_reason = "Conditional check failed: " + row_condition + " (excluding years after " + last_eia860_year|string + " due to EIA-860M limitations)" %}
        {% else %}
            {% set combined_condition = row_condition %}
            {% set failure_reason = "Conditional check failed: " + row_condition %}
        {% endif %}
        {% set check %}
            SELECT
                '{{ model.name }}' as table_name,
                '{{ column_name }}' as failing_column,
                '{{ failure_reason }}' as failure_reason,
                '{{ row_condition | replace("'", "''") }}' as row_condition,
                COUNT(*) as total_rows_matching_condition,
                COUNT({{ column_name }}) as non_null_count
            FROM {{ model }}
            WHERE {{ combined_condition }}
            HAVING COUNT(*) > 0 AND COUNT({{ column_name }}) = 0
        {% endset %}
    {% else %}
        {% if ignore_eia860m_nulls and last_eia860_year %}
            {% set check %}
                SELECT
                    '{{ model.name }}' as table_name,
                    '{{ column_name }}' as failing_column,
                    'Column is entirely NULL (excluding years after {{ last_eia860_year }} due to EIA-860M limitations)' as failure_reason,
                    'EXTRACT(year FROM report_date) <= {{ last_eia860_year }}' as row_condition,
                    COUNT(*) as total_rows_matching_condition,
                    COUNT({{ column_name }}) as non_null_count
                FROM {{ model }}
                WHERE EXTRACT(year FROM report_date) <= {{ last_eia860_year }}
                HAVING COUNT({{ column_name }}) = 0
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
