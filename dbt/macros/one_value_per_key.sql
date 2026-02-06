{% test one_value_per_key(model, column_name, other_column_name) %}
-- This test fails if any value of {{ column_name }} maps to more than
-- one distinct value of {{ other_column_name }}.
with failures as (
    select
        {{ column_name }} as key_value,
        count(distinct {{ other_column_name }}) as distinct_count
    from {{ model }}
    where {{ column_name }} is not null
    group by {{ column_name }}
    having count(distinct {{ other_column_name }}) > 1
)

select *
from failures
{% endtest %}
