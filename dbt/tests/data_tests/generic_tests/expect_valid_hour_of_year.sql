{% test expect_valid_hour_of_year(model) %}
select
*
from {{ model }}
where (datepart('hr', datetime_utc) + ((datepart('dayofyear', datetime_utc)-1)*24)+1) != hour_of_year

{% endtest %}
