{#-
    Overall retained fraction of original generation_fuel data, by report_year and
    metric (unpivoted into one row per report_year/metric combination), for
    checking that no *individual* report_year's retention is too far from 1.0.
    This deliberately checks each report_year independently rather than an
    all-years aggregate, since a single bad year can get lost in a bulk average
    across many good years. See validate_eia923__generation_fuel_allocation's
    header comment for which report_years are excluded and why.
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
    allocated_{{ metric }} / nullif(original_{{ metric }}, 0) as retained_fraction
from by_year
{% if not loop.last %}union all{% endif %}
{% endfor %}
