with NukeGens as (
    select distinct
        plant_id_eia,
        generator_id,
        'generator' as generator
    from {{ source('pudl', '_out_eia__yearly_generators') }}
    where energy_source_code_1 = 'NUC'
), NukeGenerationFuel as (
    select
        plant_id_eia,
        nuclear_unit_id as generator_id
    from {{ source('pudl', 'core_eia923__monthly_generation_fuel_nuclear') }}
    where nuclear_unit_id != 'UNK'
), NukeMatches as (
    select *
    from NukeGenerationFuel left join NukeGens using (plant_id_eia, generator_id)
) select *
from NukeMatches
where generator is null
