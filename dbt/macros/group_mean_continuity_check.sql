{% test group_mean_continuity_check(model, ordered_group_column, thresholds, n_outliers_allowed=0) %}

with GroupMean as (
    select
    {{ ordered_group_column }},
    {% for threshold in thresholds %}
    avg({{ threshold.column }}) as mean_{{ loop.index }} {%- if not loop.last -%},{%- endif -%}
    {% endfor %}
    from {{ model }}
    group by {{ ordered_group_column }}
),
Continuity as (
    select
    {{ ordered_group_column }},
    {% for threshold in thresholds %}
    mean_{{ loop.index }} as newer_value_{{ loop.index }},
    LAG(mean_{{ loop.index }}) OVER w as older_value_{{ loop.index }} {%- if not loop.last -%},{%- endif -%}
    {% endfor %}
    from GroupMean
    window w as (order by {{ ordered_group_column }})
),
{% for threshold in thresholds %}
Violations_{{ loop.index }} as (
    select
    {{ ordered_group_column }},
    '{{ threshold.column }}' as 'column_name',
    abs(newer_value_{{ loop.index }} - older_value_{{ loop.index }})/older_value_{{ loop.index }} as fr_change
    from Continuity
    where newer_value_{{ loop.index }} is not null and older_value_{{ loop.index }} is not null and fr_change >= {{ threshold.max_fr_change }}
    limit 1e6 offset {{ n_outliers_allowed }}
){%- if not loop.last -%},{%- endif -%}
{% endfor %}
{% for threshold in thresholds %}
select * from
Violations_{{ loop.index }}
{% if not loop.last %} union all {% endif %}
{% endfor %}

{% endtest %}
