{% test not_all_null(model, column_name) %}

WITH not_null_count AS (
    SELECT COUNT(*) as not_null_count
    FROM {{ model }}
    WHERE {{ column_name }} IS NOT NULL
)

SELECT not_null_count FROM not_null_count WHERE not_null_count = 0

{% endtest %}
