select
capacity_factor,
report_year,
plant_capability_mw,
capacity_mw,
water_limited_capacity_mw / capacity_mw as water_limited_ratio,
not_water_limited_capacity_mw / capacity_mw as not_water_limited_ratio,
peak_demand_mw / capacity_mw as peak_demand_ratio,
plant_capability_mw / capacity_mw as capability_ratio,
from {{ source('pudl', 'out_ferc1__yearly_steam_plants_sched402') }}
