{% macro weighted_quantile(model, column_name, weight_col, quantile, row_condition=None) %}

WITH CumulativeWeights AS (
    SELECT
        {{ column_name }},
        {{ weight_col }},
        SUM({{ weight_col }}) OVER (ORDER BY {{ column_name }}) AS cumulative_weight,
        SUM({{ weight_col }}) OVER () AS total_weight
    FROM {{ model }}
    WHERE ({{ column_name }} IS NOT NULL AND {{ weight_col }} IS NOT NULL)
    {% if row_condition %}and {{ row_condition }}{% endif %}
),
QuantileData AS (
    SELECT
        {{ column_name }},
        {{ weight_col }},
        cumulative_weight,
        total_weight,
        (cumulative_weight - 0.5 * {{ weight_col }}) / total_weight AS cumulative_probability
    FROM CumulativeWeights
),
QuantilePoints AS (
    SELECT
        {{ column_name }} AS lower_value,
        LEAD({{ column_name }}) OVER (ORDER BY cumulative_probability) AS upper_value,
        cumulative_probability AS lower_prob,
        LEAD(cumulative_probability) OVER (ORDER BY cumulative_probability) AS upper_prob
    FROM QuantileData
),
InterpolatedQuantile AS (
    SELECT
        CASE
            WHEN {{ quantile }} = 0 THEN (SELECT MIN({{ column_name }}) FROM QuantileData)  -- Handling quantile = 0
            WHEN {{ quantile }} = 1 THEN (SELECT MAX({{ column_name }}) FROM QuantileData)  -- Handling quantile = 1
            WHEN upper_value is null then lower_value
            ELSE lower_value +
                 (upper_value - lower_value) *
                 (({{ quantile }} - lower_prob) / (upper_prob - lower_prob))  -- Regular interpolation
        END AS interpolated_value
    FROM QuantilePoints
    WHERE lower_prob <= {{ quantile }} AND upper_prob >= {{ quantile }}
    LIMIT 1
)
SELECT interpolated_value
FROM InterpolatedQuantile

{%  endmacro %}
