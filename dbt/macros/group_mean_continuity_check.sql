{% test group_mean_continuity_check(model, column_name, ordered_group_column, max_fr_change, n_outliers_allowed=0) %}

with GroupMean as (
    select
    {{ ordered_group_column }},
    avg({{ column_name }}) as mean
    from {{ model }}
    group by {{ ordered_group_column }}
),
Continuity as (
    select
    {{ ordered_group_column }},
    mean as newer_value,
    LAG(mean) OVER (order by {{ ordered_group_column }}) as older_value
    from GroupMean
)
select
{{ ordered_group_column }},
abs(newer_value - older_value)/older_value as fr_change
from Continuity
where newer_value is not null and older_value is not null and fr_change >= {{ max_fr_change }}
limit 1e6 offset {{ n_outliers_allowed }}

{% endtest %}
