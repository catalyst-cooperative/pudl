{% test expect_matching_primary_fuel_by_cost_and_mmbtu(model) %}
with MismatchMicro as (
select
case
    when (
        (primary_fuel_by_cost != primary_fuel_by_mmbtu)
        and (primary_fuel_by_cost is not null)
        and (primary_fuel_by_mmbtu is not null)
    ) then 1
    else 0
end as mismatch,
1 as base_count
from {{ model }}
), MismatchSummary as (
select sum(mismatch) as mismatch_count, sum(base_count) as base_count from MismatchMicro
)
select * from MismatchSummary where mismatch_count / base_count > 0.05

{% endtest %}
