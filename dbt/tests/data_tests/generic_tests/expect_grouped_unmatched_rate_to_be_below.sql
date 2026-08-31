{% test expect_grouped_unmatched_rate_to_be_below(
    model,
    group_by_column,
    key_column,
    other_table,
    other_key_column,
    other_match_column,
    max_unmatched_ratio
) %}

with model_keys as (
    select distinct
        {{ group_by_column }} as group_value,
        {{ key_column }} as key_value
    from {{ model }}
    where {{ key_column }} is not null
),

matches as (
    select distinct {{ other_key_column }} as key_value
    from {{ other_table }}
    where {{ other_key_column }} is not null
        and {{ other_match_column }} is not null
),

group_stats as (
    select
        model_keys.group_value,
        count(*) as total_keys,
        count(matches.key_value) as matched_keys
    from model_keys
    left join matches on model_keys.key_value = matches.key_value
    group by model_keys.group_value
)

select
    group_value,
    total_keys,
    matched_keys,
    total_keys - matched_keys as unmatched_keys,
    round(1.0 - (matched_keys / total_keys::float), 4) as unmatched_ratio
from group_stats
where (1.0 - (matched_keys / total_keys::float)) > {{ max_unmatched_ratio }}

{% endtest %}
