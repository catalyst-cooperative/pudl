{% test expect_unique_combination(model, columns) %}
select
    {% for column_i in columns %}{{ column_i }},
    {% endfor %}
    count(1) as frequency
from {{ model }}
where
    {% for column_i in columns %}{{ column_i }} is not null
    {% if not loop.last %} and{% endif %}
    {% endfor %}
group by all
having frequency > 1

{% endtest %}
