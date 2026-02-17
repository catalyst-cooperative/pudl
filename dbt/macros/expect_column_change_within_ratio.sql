{% test expect_column_change_within_ratio(model, column_name, group_by, order_by, max_pct_change, row_condition=None) %}

{{ config(severity = 'error', warn_if  = '> 0', error_if = '> 0') }}

with ordered as (

    select
        {{ group_by }}    as grp,
        {{ order_by }}    as ts,
        {{ column_name }} as val
    from {{ model }}
    {% if row_condition is not none %}
    where {{ row_condition }}
    {% endif %}

),

with_lag as (

    select
        grp,
        ts,
        val,
        lag(val) over (
            partition by grp
            order by ts
        ) as prev_val
    from ordered

),

violations as (

    select
        grp,
        ts,
        val,
        prev_val,
        case
            when prev_val = 0 then null
            else abs( (val - prev_val) / prev_val )
        end as pct_change
    from with_lag
    where prev_val is not null
)

select *
from violations
where pct_change > {{ max_pct_change }}

{% endtest %}
