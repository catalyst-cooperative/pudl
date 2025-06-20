{% test expect_missingness_between(
    model,
    column_name,
    lower_bound=0,
    upper_bound=1
) %}

SELECT
    COUNT(*) AS total_records,
    COUNT(*) - COUNT({{ column_name }}) AS null_records,
    (COUNT(*) - COUNT({{ column_name }})::FLOAT) / NULLIF(COUNT(*), 0) AS null_proportion
FROM {{ model }}
HAVING null_proportion < {{ lower_bound }}
    OR null_proportion > {{ upper_bound }}

{% endtest %}
