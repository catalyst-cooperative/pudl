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
