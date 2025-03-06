with PlantIds as (
    select distinct
        plant_id_pudl,
        plant_id_ferc1
    from {{ source('pudl', 'out_ferc1__yearly_steam_plants_sched402') }}
), BadPlantIds as (
    select
        plant_id_ferc1,
        count(1) as pudl_id_count
    from PlantIds
    group by all
    having pudl_id_count > 1
    limit 1e6 offset 6
) select
    plant_id_ferc1,
    plant_id_pudl
from BadPlantIds left join PlantIds using(plant_id_ferc1)
