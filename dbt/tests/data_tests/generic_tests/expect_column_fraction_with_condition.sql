{% test expect_column_fraction_with_condition(
    model,
    column_name,
    numerator_row_condition,
    min_fraction,
    max_fraction
) %}

{# Validate min_fraction and max_fraction are between 0.0 and 1.0, and min_fraction
is not greater than max_fraction. Raise an error if any of these conditions are not
met. #}

{% if min_fraction < 0.0 or min_fraction > 1.0 %}
    {{ exceptions.raise_compiler_error("min_fraction must be between 0.0 and 1.0, got: " ~ min_fraction) }}
{% endif %}

{% if max_fraction < 0.0 or max_fraction > 1.0 %}
    {{ exceptions.raise_compiler_error("max_fraction must be between 0.0 and 1.0, got: " ~ max_fraction) }}
{% endif %}

{% if min_fraction > max_fraction %}
    {{ exceptions.raise_compiler_error("min_fraction (" ~ min_fraction ~ ") cannot be greater than max_fraction (" ~ max_fraction ~ ")") }}
{% endif %}

WITH fraction_check AS (
    SELECT
        SUM(CASE WHEN {{ numerator_row_condition }} THEN {{ column_name }} ELSE 0 END) * 1.0 / SUM({{ column_name }}) AS actual_fraction,
        SUM(CASE WHEN {{ numerator_row_condition }} THEN {{ column_name }} ELSE 0 END) AS condition_sum,
        SUM({{ column_name }}) AS total_sum,
    FROM {{ model }}
)

SELECT
    'Test failed: actual_fraction of {{ column_name }} where ' ||
    '{{ numerator_row_condition | replace("'", "''")}}' ||
    ' is outside of acceptable range.'
    AS failure_message,
    ROUND(actual_fraction, 4) as actual_fraction,
    {{ min_fraction }} AS min_fraction,
    {{ max_fraction }} AS max_fraction,
    ROUND(condition_sum, 4) AS condition_sum,
    ROUND(total_sum, 4) AS total_sum
FROM fraction_check
WHERE actual_fraction < {{ min_fraction }} OR actual_fraction > {{ max_fraction }}

{% endtest %}
