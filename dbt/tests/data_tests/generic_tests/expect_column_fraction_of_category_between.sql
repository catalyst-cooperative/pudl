{% test expect_column_fraction_of_category_between(
    model,
    column_name,
    category_column,
    category_value,
    lower_bound=0,
    upper_bound=1
) %}

SELECT
    SUM(WHEN {{ category_column }} =
                {% if category_value is string %}
                    '{{ category_value }}'
                {% else %}
                    {{ category_value }}
                {% endif %}
        THEN {{ column_name }} ELSE 0 END) * 1.0
    / SUM({{ column_name }}) AS fraction
FROM {{ model }}
HAVING fraction < {{ lower_bound }}
    OR fraction > {{ upper_bound }}

{% endtest %}
