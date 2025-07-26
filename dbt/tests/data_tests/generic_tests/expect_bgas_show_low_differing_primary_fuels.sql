{% test expect_bgas_show_low_differing_primary_fuels(
    model,
    bga_model,
    max_differing_fuel_rate = 0.01
) %}

-- Test the boiler generator associations.
with GensSimple as (
    select
        report_date,
        plant_id_eia,
        generator_id,
        fuel_type_code_pudl
    from {{ model }}
), BgaGens as (
    select distinct
        report_date,
        plant_id_eia,
        unit_id_pudl,
        generator_id
    from {{ bga_model }}
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
where (num_multi_fuel_units / all_units) >= {{ max_differing_fuel_rate }}
-- Failure rate for etl-fast as of Mar 2025 was 0.0087
-- (we're primarily checking that this is not getting worse)

{% endtest %}
