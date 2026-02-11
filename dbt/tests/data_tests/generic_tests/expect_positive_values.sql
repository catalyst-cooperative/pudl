{% test expect_positive_values(model, positive_columns) %}

select *
from {{ model }}
where
    {% for col in positive_columns %}
        (
            {{ col }} is not null
            and {{ col }} < 0
        )
        {% if not loop.last %} or {% endif %}
    {% endfor %}

{% endtest %}
