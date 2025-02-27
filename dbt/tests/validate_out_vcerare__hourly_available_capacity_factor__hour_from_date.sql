select
*
from {{ source('pudl', 'out_vcerare__hourly_available_capacity_factor') }}
where (datepart('hr', datetime_utc) + ((datepart('dayofyear', datetime_utc)-1)*24)+1) != hour_of_year
