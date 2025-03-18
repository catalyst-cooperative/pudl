{% test check_column_correlation(model, column1, column2, correlation_coef) %}

WITH
    correlation_calc as (
        SELECT
            corr({{ column1 }}, {{ column2 }}) as correlation
        FROM {{ model }}
    )

SELECT *
FROM correlation_calc
WHERE correlation < {{ correlation_coef }}

{% endtest %}
