select
county_id_fips, datetime_utc, count(1) as frequency
from {{ source('pudl', 'out_vcerare__hourly_available_capacity_factor') }}
where county_id_fips is not null
group by all
having frequency > 1
