-- Test the boiler generator associations.
with GensSimple as (
    select
        report_date,
        plant_id_eia,
        generator_id,
        fuel_type_code_pudl
    from {{ source('pudl', 'out_eia__yearly_generators') }}
), BgaGens as (
    select distinct
        report_date,
        plant_id_eia,
        unit_id_pudl,
        generator_id
    from {{ source('pudl', 'core_eia860__assn_boiler_generator') }}
), UnitsSimple as (
    select distinct
        report_date,
        plant_id_eia,
        fuel_type_code_pudl,
        unit_id_pudl
    from GensSimple join BgaGens using (report_date, plant_id_eia, generator_id)
), UnitsFuelCount as (
    select
        report_date,
        plant_id_eia,
        unit_id_pudl,
        count(1) as fuel_type_count
    from UnitsSimple group by all
), DifferingPrimaryFuels as (
    select
        case when fuel_type_count > 1 then 1
        else 0
        end as multi_fuel_units,
        1 as all_units
    from UnitsFuelCount
), DifferingPrimaryFuelRate as (
    select
        sum(multi_fuel_units) as num_multi_fuel_units,
        sum(all_units) as all_units
    from DifferingPrimaryFuels
)
select * from DifferingPrimaryFuelRate
where (num_multi_fuel_units / all_units) >= 0.01
-- Failure rate for etl-fast as of Mar 2025 was 0.0087
-- (we're primarily checking that this is not getting worse)
