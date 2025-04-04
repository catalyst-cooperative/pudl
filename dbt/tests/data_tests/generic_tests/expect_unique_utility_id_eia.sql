{% test expect_unique_utility_id_eia(model, n_acceptable_failures=0) %}
with OperatorCheck as (
    select
        report_date,
        plant_id_eia,
        generator_id,
        count(distinct operator_utility_id_eia) as operator_utility_id_eia
    from {{ model }}
    group by
        report_date,
        plant_id_eia,
        generator_id
)
select * from OperatorCheck where operator_utility_id_eia > 1
limit 1e6 offset {{ n_acceptable_failures }} --

{% endtest %}
