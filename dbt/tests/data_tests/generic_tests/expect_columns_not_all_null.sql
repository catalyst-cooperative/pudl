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

{% set year_in_eia860_years = "EXTRACT(year FROM report_date) <= " + last_eia860_year|string %}

{% if execute %}
    {% set columns_result = run_query(get_columns_query) %}
    {% set column_names = columns_result.columns[0].values() %}

    {# Validate that report_date column exists if ignore_eia860m_nulls is enabled #}
    {% if ignore_eia860m_nulls and 'report_date' not in column_names %}
        {{ exceptions.raise_compiler_error("ignore_eia860m_nulls=true requires a 'report_date' column in the table being tested, but 'report_date' was not found in " + model.name) }}
    {% endif %}
{% else %}
    {% set column_names = [] %}
{% endif %}

{% set checks = [] %}
{% for column_name in column_names %}
    {# Determine the WHERE condition for this column #}
    {% if column_name in row_conditions %}
        {% set base_condition = row_conditions[column_name] %}
    {% else %}
        {% set base_condition = "TRUE" %}
    {% endif %}

    {# Add EIA-860M year exclusion if needed #}
    {% if ignore_eia860m_nulls and last_eia860_year %}
        {% set where_condition = "(" + base_condition + ") AND (" + year_in_eia860_years + ")" %}
        {% set exclusion_note = " (excluding years after " + last_eia860_year|string + " due to EIA-860M limitations)" %}
    {% else %}
        {% set where_condition = base_condition %}
        {% set exclusion_note = "" %}
    {% endif %}

    {# Set failure reason and row_condition display #}
    {% if column_name in row_conditions %}
        {% set failure_reason = "Conditional check failed: " + row_conditions[column_name] + exclusion_note %}
        {% set row_condition_display = row_conditions[column_name] | replace("'", "''") %}
    {% else %}
        {% set failure_reason = "Column is entirely NULL" + exclusion_note %}
        {% set row_condition_display = "N/A (entire table)" if not exclusion_note else year_in_eia860_years %}
    {% endif %}

    {% set check %}
        SELECT
            '{{ model.name }}' as table_name,
            '{{ column_name }}' as failing_column,
            '{{ failure_reason }}' as failure_reason,
            '{{ row_condition_display }}' as row_condition,
            COUNT(*) as total_rows_matching_condition,
            COUNT({{ column_name }}) as non_null_count
        FROM {{ model }}
        WHERE {{ where_condition }}
        HAVING COUNT(*) > 0 AND COUNT({{ column_name }}) = 0
    {% endset %}
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
