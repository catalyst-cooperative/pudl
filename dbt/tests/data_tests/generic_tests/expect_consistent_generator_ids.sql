{% test expect_consistent_generator_ids(model, n_acceptable_failures=0) %}
-- Check if there are any plants that report inconsistent generator IDs.
--
-- There are some instances in which a plant will report generator IDs differently in
-- different years, such that the IDs differ only in terms of the case of letters, or
-- non-alphanumeric characters. This test identifies them. We haven't fixed them yet.
with GenIds as (
    select distinct
        plant_id_eia,
        generator_id,
        regexp_replace(upper(generator_id), '[^a-zA-Z0-9]', '') as simple_id
    from {{ model }}
), MultipleIds as (
    select
        plant_id_eia,
        simple_id,
        count(distinct generator_id) as generator_id_count
    from GenIds
    group by plant_id_eia, simple_id
    having generator_id_count > 1
) select *
from GenIds join MultipleIds using (plant_id_eia, simple_id)
order by plant_id_eia, simple_id
limit 1e6 offset {{ n_acceptable_failures }}

{% endtest %}
