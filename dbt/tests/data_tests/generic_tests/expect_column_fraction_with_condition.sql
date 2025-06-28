{% test expect_column_fraction_with_condition(
    model,
    column_name,
    column_condition,
    expected_fraction,
    tol=0.02
) %}

SELECT
    SUM(
        CASE
            WHEN {{ column_condition }}
            THEN {{ column_name }} ELSE 0
        END
    )* 1.0
    / SUM({{ column_name }}) AS actual_fraction
FROM {{ model }}
HAVING abs(actual_fraction - {{ expected_fraction }}) > {{ tol }}

{% endtest %}
