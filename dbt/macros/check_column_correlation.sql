{% test check_column_correlation(model, column1, column2, correlation_coef) %}

WITH
    corr as (
        SELECT
            corr({{ column1 }}, {{ column2 }}) as correlation
        FROM {{ model }}
    )

SELECT *
FROM corr
WHERE correlation < {{ correlation_coef }}

{% endtest %}
