{% test expect_col1_greater_or_equal_to_col2(model, col1, col2) %}

select *
from {{ model }}
where
    {{ col1 }} is not null
    and {{ col2 }} is not null
    and {{ col1 }} < {{ col2 }}

{% endtest %}
