{% test one_value_per_group(model, column_name, group_by_columns) %}
-- This test fails if any group defined by {{ group_by_columns }} contains more
-- than one distinct value of {{ column_name }}. Nulls are ignored, so a group
-- with a single non-null value alongside nulls passes.
{%- set group_by = group_by_columns | join(", ") %}
with failures as (
    select
        {{ group_by }},
        count(distinct {{ column_name }}) as distinct_count
    from {{ model }}
    group by {{ group_by }}
    having count(distinct {{ column_name }}) > 1
)

select *
from failures
{% endtest %}
