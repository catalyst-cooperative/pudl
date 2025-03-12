{% test expect_fgd_cost_totals_to_agree(model) %}
with Discrepancies as (
    select
        case
            when (fgd_structure_cost is not null) or (fgd_other_cost is not null) or (sludge_disposal_cost is not null) then coalesce(fgd_structure_cost, 0) + coalesce(fgd_other_cost, 0) + coalesce(sludge_disposal_cost, 0)
            else null
        end as sum_cost,
        abs(sum_cost - total_fgd_equipment_cost) / total_fgd_equipment_cost as discrepancy,
        case when (discrepancy > 0.01) and (sum_cost != 0 or total_fgd_equipment_cost != 0) then 1 -- additional sum_cost and total_ constraints drop any nan entries which would otherwise match
        else 0
        end as exceeds_threshold
    from {{ model }}
), DiscrepancyCounts as (
    select
        exceeds_threshold,
        count(1) over () as n,
        count(1) over (partition by exceeds_threshold) as threshold_n
    from Discrepancies
), TargetRate as (
    select threshold_n / n as rate
    from DiscrepancyCounts
    where exceeds_threshold=1
    limit 1
) select * from TargetRate where rate >= 0.01

{% endtest %}
