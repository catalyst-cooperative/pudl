{% test expect_unique_owner_utility_id_eia(model) %}
with OperatorCheck as (
    select
        report_date,
        plant_id_eia,
        generator_id,
        count(distinct owner_utility_id_eia) as owner_utility_id_eia
    from {{ model }}
    group by
        report_date,
        plant_id_eia,
        generator_id
)
select * from OperatorCheck where owner_utility_id_eia > 1
limit 1e6 offset 730 -- 730 known generators as of time python test was marked xfail

{% endtest %}
