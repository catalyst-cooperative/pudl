{% test expect_unimputed_values_are_unchanged(
    model,
    original,
    imputed,
    imputation_code,
    atol=1,
    rtol=0.001
) %}
SELECT * FROM {{ model }}
WHERE {{ imputation_code }} IS NULL
AND (
    (abs({{ original }} - {{ imputed }}) > {{ atol }})
    OR (abs({{ original }} - {{ imputed }}) / greatest(abs({{ original }}), abs({{ imputed }})) > {{ rtol }})
    OR ({{ original }} IS NULL AND {{ imputed }} IS NOT NULL)
    OR ({{ original }} IS NOT NULL AND {{ imputed }} IS NULL)
)

{% endtest %}
