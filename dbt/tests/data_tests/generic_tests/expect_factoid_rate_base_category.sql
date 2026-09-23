-- Factoids that end with a rate-base category should be tagged with that
-- category. `accumulated_depreciation` is a known exception because its
-- category is determined by `plant_function`.
{% test expect_factoid_rate_base_category(
    model,
    column_name,
    row_condition = None
) %}

WITH expected_rate_base_categories AS (
    SELECT DISTINCT {{ column_name }} AS expected_rate_base_category
    FROM {{ model }}
    WHERE expected_rate_base_category IS NOT NULL
),
category_factoids AS (
    SELECT
        rate_base.*,
        categories.expected_rate_base_category
    FROM {{ model }} AS rate_base
    CROSS JOIN expected_rate_base_categories AS categories
    WHERE ends_with(rate_base.xbrl_factoid, categories.expected_rate_base_category)
    {% if row_condition is not none %}
      AND {{ row_condition }}
    {% endif %}
)
SELECT *
FROM category_factoids
WHERE {{ column_name }} IS DISTINCT FROM expected_rate_base_category

{% endtest %}
