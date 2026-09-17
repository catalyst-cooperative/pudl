{{ config(materialized='view') }}

-- One row per (year, plant_id_eia, emissions_unit_id_epa) unit that ever reports to
-- CEMS in that year, flagging whether that unit found a non-null generator_id match
-- in the EPA/EIA subplant crosswalk. Exists solely so that per-year match-rate
-- thresholds can be checked below with a plain dbt_utils.not_null_proportion test --
-- core_epacems__hourly_emissions has no generator_id of its own to test directly, and
-- core_epa__assn_eia_epacamd_subplant_ids has no year column to group by.

with cems_units as (
    select distinct
        year,
        plant_id_eia,
        emissions_unit_id_epa
    from {{ source('pudl', 'core_epacems__hourly_emissions') }}
    where plant_id_eia is not null and emissions_unit_id_epa is not null
),

matched_units as (
    select distinct
        plant_id_eia,
        emissions_unit_id_epa
    from {{ source('pudl', 'core_epa__assn_eia_epacamd_subplant_ids') }}
    where plant_id_eia is not null
        and emissions_unit_id_epa is not null
        and generator_id is not null
)

select
    cems_units.year,
    cems_units.plant_id_eia,
    cems_units.emissions_unit_id_epa,
    matched_units.plant_id_eia as matched_plant_id_eia
from cems_units
left join matched_units
    on cems_units.plant_id_eia = matched_units.plant_id_eia
    and cems_units.emissions_unit_id_epa = matched_units.emissions_unit_id_epa
