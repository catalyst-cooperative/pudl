-- Factoids that end with a rate-base category should be tagged with that
-- category. `accumulated_depreciation` is a known exception because its
-- category is determined by `plant_function`.
{% test expect_factoid_rate_base_category(model) %}

WITH rate_base_categories AS (
    SELECT DISTINCT rate_base_category
    FROM {{ model }}
    WHERE rate_base_category IS NOT NULL
),
category_factoids AS (
    SELECT
        rate_base.*,
        categories.rate_base_category AS expected_rate_base_category
    FROM {{ model }} AS rate_base
    CROSS JOIN rate_base_categories AS categories
    WHERE rate_base.xbrl_factoid != 'accumulated_depreciation'
      AND rate_base.rate_base_category NOT IN ('asset_retirement_costs', 'other_deferred_debits_and_credits')
      AND ends_with(rate_base.xbrl_factoid, categories.rate_base_category)
)
SELECT *
FROM category_factoids
WHERE rate_base_category IS DISTINCT FROM expected_rate_base_category

{% endtest %}
