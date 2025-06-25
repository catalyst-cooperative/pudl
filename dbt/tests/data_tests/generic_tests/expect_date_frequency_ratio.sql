{% test expect_date_frequency_ratio(
    model,
    compare_model,
    multiplier,
    date_column='report_date'
) %}

WITH model_years AS (
    SELECT
        EXTRACT(YEAR FROM {{ date_column }}) as year,
        COUNT(DISTINCT {{ date_column }}) as date_count
    FROM {{ model }}
    WHERE {{ date_column }} IS NOT NULL
    {% if 'data_maturity' in adapter.get_columns_in_relation(model) %}
        AND (data_maturity != 'incremental_ytd' OR data_maturity IS NULL)
    {% endif %}
    GROUP BY EXTRACT(YEAR FROM {{ date_column }})
),

compare_years AS (
    SELECT
        EXTRACT(YEAR FROM {{ date_column }}) as year,
        COUNT(DISTINCT {{ date_column }}) as date_count
    FROM {{ compare_model }}
    WHERE {{ date_column }} IS NOT NULL
    {% if 'data_maturity' in adapter.get_columns_in_relation(model) %}
        AND (data_maturity != 'incremental_ytd' OR data_maturity IS NULL)
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
