{% test group_mean_continuity_check(model, column_name, group_column, max_pct_change) %}

with GroupMean as (
    select
    {{ group_column }},
    avg({{ column_name }}) as mean
    from {{ model }}
    group by {{ group_column }}
),
Continuity as (
    select
    {{ group_column }},
    mean as newer_value,
    LAG(mean) OVER (order by {{ group_column }}) as older_value
    from GroupMean
)
select
{{ group_column }},
abs(newer_value - older_value)/older_value as pct_change
from Continuity
where newer_value is not null and older_value is not null and pct_change >= {{ max_pct_change }}

{% endtest %}
