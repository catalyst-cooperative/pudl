select
plant_id_ferc1, report_year, count(1) as frequency
from {{ source('pudl', 'out_ferc1__yearly_steam_plants_sched402') }}
group by plant_id_ferc1, report_year
having frequency > 1
