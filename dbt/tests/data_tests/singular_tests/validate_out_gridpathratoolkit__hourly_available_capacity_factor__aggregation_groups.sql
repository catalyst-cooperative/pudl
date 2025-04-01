-- Check that every capacity factor aggregation key appears in the aggregations.
--
-- This isn't a normal foreign-key relationship, since the aggregation group isn't the
-- primary key in the aggregation tables, and is not unique in either of these tables,
-- but if an aggregation group appears in the capacity factor time series and never
-- appears in the aggregation table, then something is wrong.
with AggGroups as (
    select distinct
        aggregation_group,
        'found' as found_assn
    from {{ source('pudl', 'core_gridpathratoolkit__assn_generator_aggregation_group') }}
) select distinct
    aggregation_group,
    found_assn
from {{ source('pudl', 'out_gridpathratoolkit__hourly_available_capacity_factor') }}
left join AggGroups using (aggregation_group)
where found_assn is null
