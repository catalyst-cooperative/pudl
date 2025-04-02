{% test expect_valid_plantparts_ownership_fraction(model) %}

{% set ownership_keys = ['energy_source_code_1', 'ferc_acct_name', 'generator_id', 'generator_operating_year', 'operational_status_pudl', 'plant_id_eia', 'prime_mover_code', 'report_date', 'technology_description', 'unit_id_pudl', 'plant_part', 'ownership_record_type'] %}

with FractionDefined as (
    select *
    from {{ model }}
    where fraction_owned is not null
), CapacityDefined as (
    select *
    from {{ model }}
    where capacity_mw is not null
), FractionSums as (
    select
        {{ ','.join(ownership_keys) }},
        sum(fraction_owned) as fraction_owned,
    from FractionDefined
    group by all
), CapacitySums as (
    select
        {{ ','.join(ownership_keys) }},
        sum(capacity_mw) as capacity_mw,
    from CapacityDefined
    group by all
), OwnershipSums as (
    select *
    from FractionSums full outer join CapacitySums using (
        {{ ','.join(ownership_keys) }}
    )
), BadOwnership as (
    select
        *
    from OwnershipSums
    where
        (abs(fraction_owned - 1) > 1e-6) and
        (capacity_mw is not null) and
        (capacity_mw != 0) and
        (ownership_record_type = 'owned')
), BadZeroes as (
    select
        *
    from OwnershipSums
    where
        (capacity_mw = 0) and
        (fraction_owned = 0)
    limit 1e6 offset 60 -- max 60 entries
) select * from BadOwnership
union all
select * from BadZeroes

{% endtest %}
