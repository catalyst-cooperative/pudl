{% test expect_unique_utility_id_eia(model, n_acceptable_failures=0) %}
with OperatorCheck as (
    select
        report_date,
        plant_id_eia,
        generator_id,
        count(distinct utility_id_pudl) as utility_id_pudl
    from {{ model }}
    group by
        report_date,
        plant_id_eia,
        generator_id
)
select * from OperatorCheck where utility_id_pudl > 1
limit 1e6 offset {{ n_acceptable_failures }} --

{% endtest %}
