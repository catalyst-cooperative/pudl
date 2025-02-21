
select
{% for fuel_type in ["gas", "oil", "coal"] %}
    {{ fuel_type }}_fraction_cost * fuel_cost / ({{ fuel_type }}_fraction_mmbtu * fuel_mmbtu) as {{ fuel_type }}_cost_per_mmbtu,
{% endfor %}
from {{ source('pudl', 'out_ferc1__yearly_steam_plants_fuel_by_plant_sched402') }}
