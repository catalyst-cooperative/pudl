{% test expect_sum_to_target_discrepancy_rate_less_than(model, sum_columns, target_column, discrepancy_threshold, max_discrepancy_rate) %}
with Discrepancies as (
    select
        ({% for c in sum_columns %}
        coalesce({{ c }}, 0) {% if not loop.last %}+ {% endif %}
        {% endfor %} - {{ target_column }}) / {{ target_column }} as discrepancy,
        discrepancy is not null and discrepancy > {{ discrepancy_threshold }} as bad_discrepancy
    from {{ model }}
), BadDiscrepancies as (
    select * from Discrepancies where discrepancy is not null and discrepancy > {{ discrepancy_threshold }}
), DiscrepancyRate as (
    select
        bad_discrepancy,
        count(*) over () as total_count,
        count(*) over (partition by bad_discrepancy) as discrepancy_count,
        discrepancy_count / total_count as discrepancy_rate
    from Discrepancies
    order by bad_discrepancy desc
    limit 1
)
select * from DiscrepancyRate
where discrepancy_rate > {{ max_discrepancy_rate }}

{% endtest %}
