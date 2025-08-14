{% test expect_nuclear_units_are_generators(model, generator_model) %}

with NukeGens as (
    -- Get all generators with nuclear fuel
    select distinct
        plant_id_eia,
        generator_id,
        'generator' as generator
    from {{ generator_model }}
    where energy_source_code_1 = 'NUC'
), NukeGenerationFuel as (
    -- Get generator IDs for nuclear units that have them
    select
        plant_id_eia,
        nuclear_unit_id as generator_id
    from {{ model }}
    where nuclear_unit_id != 'UNK'
), NukeMatches as (
    -- Want generators from NukeGenerationFuel to be a subset of the ones
    -- from NukeGens, so do a left join and look for nulls on the right
    select *
    from NukeGenerationFuel left join NukeGens using (plant_id_eia, generator_id)
) select *
from NukeMatches
where generator is null

{% endtest %}
