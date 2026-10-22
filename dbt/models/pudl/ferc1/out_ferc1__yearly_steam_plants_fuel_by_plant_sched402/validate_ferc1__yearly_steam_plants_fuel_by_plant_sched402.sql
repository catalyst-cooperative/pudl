{% set fuel_types = ["gas", "oil", "coal", "waste", "nuclear"] %}

select
report_year, plant_id_pudl,
{% for fraction_type in ["cost", "mmbtu"] %}
    case when -- if any values exist, use them
({%- for fuel_type in fuel_types -%}
    {{ fuel_type }}_fraction_{{ fraction_type }} is not null {% if not loop.last %}or {% endif %}
{%- endfor -%}) then
{% for fuel_type in fuel_types %}
    coalesce({{ fuel_type }}_fraction_{{ fraction_type }}, 0) {% if not loop.last %}+ {% endif %}
{% endfor %}
    else 1.0 -- otherwise trivially pass this row
    end as total_{{ fraction_type }}_fraction,
{% endfor %}
{% for fuel_type in fuel_types %}
    {{ fuel_type }}_fraction_cost * fuel_cost / ({{ fuel_type }}_fraction_mmbtu * fuel_mmbtu) as {{ fuel_type }}_cost_per_mmbtu,
{% endfor %}
from {{ source('pudl', 'out_ferc1__yearly_steam_plants_fuel_by_plant_sched402') }}
