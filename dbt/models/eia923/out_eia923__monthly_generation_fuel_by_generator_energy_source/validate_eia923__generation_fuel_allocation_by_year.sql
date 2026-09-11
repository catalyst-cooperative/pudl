{#-
    Overall retained fraction of original generation_fuel data, by report_year and
    metric. All metrics are reshaped into a single column since they are subject to the
    same data quality thresholds. Checks each report_year independently so a single bad
    year isn't masked by good years. Note that extremely messy years (2001-2002) are
    excluded by the upstream model.
-#}
{% set metrics = ["net_generation_mwh", "fuel_consumed_mmbtu", "fuel_consumed_for_electricity_mmbtu"] %}

with by_year as (
    select
        report_year,
        {% for metric in metrics %}
        sum(original_{{ metric }}) as original_{{ metric }},
        sum(allocated_{{ metric }}) as allocated_{{ metric }}{% if not loop.last %},{% endif %}
        {% endfor %}
    from {{ ref('validate_eia923__generation_fuel_allocation') }}
    group by report_year
)

{% for metric in metrics %}
select
    '{{ metric }}' as data_column,
    report_year,
    -- Rounded to stay well above floating-point summation noise (~1e-16)
    round(allocated_{{ metric }} / nullif(original_{{ metric }}, 0), 8) as retained_fraction
from by_year
{% if not loop.last %}union all{% endif %}
{% endfor %}
