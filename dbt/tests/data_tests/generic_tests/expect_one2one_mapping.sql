{% test expect_one2one_mapping(model, every_column, should_have_only_one_column, n_acceptable=0) %}
with AllPairs as (
    select distinct
        {{ every_column }},
        {{ should_have_only_one_column }}
    from {{ model }}
), BadPairs as (
    select
        {{ every_column }},
        count(1) as {{ should_have_only_one_column }}_count
    from AllPairs
    group by all
    having {{ should_have_only_one_column }}_count > 1
    limit 1e6 offset {{ n_acceptable }}
) select
    {{ every_column }},
    {{ should_have_only_one_column }}
from BadPairs left join AllPairs using ({{ every_column }})

{% endtest %}
