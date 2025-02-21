{% test no_null_rows(model) %}

SELECT *
FROM {{ model }}
WHERE COLUMNS(*) IS NULL

{% endtest %}
