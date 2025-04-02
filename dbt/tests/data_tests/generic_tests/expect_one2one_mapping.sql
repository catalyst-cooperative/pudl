{% test expect_one2one_mapping(model, groupby_column, counted_column, n_acceptable=0) %}
with AllPairs as (
    select distinct
        {{ groupby_column }},
        {{ counted_column }}
    from {{ model }}
), BadPairs as (
    select
        {{ groupby_column }},
        count(1) as {{ counted_column }}_count
    from AllPairs
    group by all
    having {{ counted_column }}_count > 1
    limit 1e6 offset {{ n_acceptable }}
) select
    {{ groupby_column }},
    {{ counted_column }}
from BadPairs left join AllPairs using ({{ groupby_column }})

{% endtest %}
