{% test expect_columns_are_close(
    model,
    column_a,
    column_b,
    row_condition=None,
    atol=1e-8,
    rtol=1e-5
) %}
SELECT
    *,
    abs({{ column_a }} - {{ column_b }}) as error,
    ({{ atol }} + {{ rtol }} * abs({{ column_b }})) as tolerance
FROM {{ model }}
WHERE
{%- if row_condition %}
    {{ row_condition }}
AND
{% endif %}
    (
        error > tolerance
        OR ({{ column_a }} IS NULL AND {{ column_b }} IS NOT NULL)
        OR ({{ column_b }} IS NOT NULL AND {{ column_a }} IS NULL)
    )

{% endtest %}
