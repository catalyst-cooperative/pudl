{% macro weighted_quantile(model, column_name, weight_col, quantile) %}

WITH CumulativeWeights AS (
    SELECT
        {{ column_name }},
        {{ weight_col }},
        SUM({{ weight_col }}) OVER (ORDER BY {{ column_name }}) AS cumulative_weight,
        SUM({{ weight_col }}) OVER () AS total_weight
    FROM {{ model }}
    WHERE {{ column_name }} IS NOT NULL OR {{ weight_col }} IS NOT NULL
),
QuantileData AS (
    SELECT
        {{ column_name }},
        {{ weight_col }},
        cumulative_weight,
        total_weight,
        cumulative_weight / total_weight AS cumulative_probability
    FROM CumulativeWeights
)
SELECT {{ column_name }}
FROM QuantileData
WHERE cumulative_probability >= {{ quantile }}
ORDER BY {{ column_name }}
LIMIT 1

{%  endmacro %}
