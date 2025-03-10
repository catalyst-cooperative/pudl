with GenIds as (
    select distinct
        plant_id_eia,
        generator_id,
        regexp_replace(upper(generator_id), '[^a-zA-Z0-9]', '') as simple_id
    from {{ source('pudl', '_out_eia__yearly_generators') }}
), MultipleIds as (
    select
        plant_id_eia,
        simple_id,
        count(distinct generator_id) as generator_id_count
    from GenIds
    group by plant_id_eia, simple_id
    having generator_id_count > 1
) select *
from GenIds join MultipleIds using (plant_id_eia, simple_id)
order by plant_id_eia, simple_id
limit 1e6 offset 40 -- as of 2021 there were 40 known inconsistent generator IDs
