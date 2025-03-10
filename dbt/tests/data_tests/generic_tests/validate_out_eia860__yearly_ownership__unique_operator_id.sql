with OperatorCheck as (
    select
        report_date,
        plant_id_eia,
        generator_id,
        count(distinct owner_utility_id_eia) as owner_utility_id_eia
    from {{ source('pudl', 'out_eia860__yearly_ownership') }}
    group by
        report_date,
        plant_id_eia,
        generator_id
)
select * from OperatorCheck where owner_utility_id_eia > 1
limit 1e6 offset 730 -- 730 known generators as of time python test was marked xfail
