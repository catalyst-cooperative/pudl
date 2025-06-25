{% test expect_date_frequency_ratio(
    model,
    compare_model,
    multiplier,
    date_column='report_date',
    model_has_data_maturity=true,
    compare_model_has_data_maturity=true
) %}

WITH model_years AS (
    SELECT
        EXTRACT(YEAR FROM {{ date_column }}) as year,
        COUNT(DISTINCT {{ date_column }}) as date_count
    FROM {{ model }}
    WHERE {{ date_column }} IS NOT NULL
    {% if model_has_data_maturity %}
        AND (data_maturity NOT IN ('incremental_ytd', 'monthly_update') OR data_maturity IS NULL)
    {% endif %}
    GROUP BY EXTRACT(YEAR FROM {{ date_column }})
),

compare_years AS (
    SELECT
        EXTRACT(YEAR FROM {{ date_column }}) as year,
        COUNT(DISTINCT {{ date_column }}) as date_count
    FROM {{ compare_model }}
    WHERE {{ date_column }} IS NOT NULL
    {% if compare_model_has_data_maturity %}
        AND (data_maturity NOT IN ('incremental_ytd', 'monthly_update') OR data_maturity IS NULL)
    {% endif %}
    GROUP BY EXTRACT(YEAR FROM {{ date_column }})
),

totals AS (
    SELECT
        COALESCE(SUM(m.date_count), 0) as model_total,
        COALESCE(SUM(c.date_count), 0) as compare_total
    FROM model_years m
    INNER JOIN compare_years c ON m.year = c.year
)

-- Test fails when totals don't match expected ratio
SELECT
    model_total,
    compare_total,
    {{ multiplier }} as expected_multiplier,
    'Expected ' || (compare_total * {{ multiplier }}) ||
    ' = ' || compare_total || ' * ' || {{ multiplier }} ||
    ', but found ' || model_total
    as failure_reason
FROM totals
WHERE model_total != compare_total * {{ multiplier }}

{% endtest %}
