{% test expect_column_fraction_with_condition(
    model,
    column_name,
    row_condition,
    min_fraction,
    max_fraction
) %}

WITH fraction_check AS (
    SELECT
        SUM(CASE WHEN {{ row_condition }} THEN {{ column_name }} ELSE 0 END) * 1.0 / SUM({{ column_name }}) AS actual_fraction,
        SUM(CASE WHEN {{ row_condition }} THEN {{ column_name }} ELSE 0 END) AS condition_sum,
        SUM({{ column_name }}) AS total_sum,
    FROM {{ model }}
)

SELECT
    'Test failed: actual_fraction of {{ column_name }} where ' ||
    '{{ row_condition | replace("'", "''")}}' ||
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
