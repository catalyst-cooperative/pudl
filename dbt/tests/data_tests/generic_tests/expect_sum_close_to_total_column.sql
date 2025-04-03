{% test expect_sum_close_to_total_column(model, sum_columns, total_column, discrepancy_threshold, max_discrepancy_rate) %}
with Discrepancies as (
    select
        ({% for c in sum_columns %}
        coalesce({{ c }}, 0) {% if not loop.last %}+ {% endif %}
        {% endfor %}) as sum_cost,
        abs(sum_cost - {{ total_column }}) / {{ total_column }} as discrepancy,
        {{ total_column }} is not null and (sum_cost != {{ total_column }}) and discrepancy > {{ discrepancy_threshold }} as bad_discrepancy
        -- null target column should not count
        -- zero target column with sum_cost!=0 (discrepancy=inf) should count
        -- zero target column with sum_cost=0 (discrepancy=nan) should not count
    from {{ model }}
), BadDiscrepancies as (
    select * from Discrepancies where bad_discrepancy is true
), DiscrepancyRate as (
    select
        bad_discrepancy,
        count(*) over () as total_count,
        count(*) over (partition by bad_discrepancy) as discrepancy_count,
        discrepancy_count / total_count as discrepancy_rate
    from Discrepancies
    order by bad_discrepancy desc -- so we get bad_discrepancy=true first, if any exist
    limit 1
)
select * from DiscrepancyRate
where bad_discrepancy is true and discrepancy_rate > {{ max_discrepancy_rate }}
-- if no bad discrepancies exist, we pass

{% endtest %}
