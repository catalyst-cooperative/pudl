{% test expect_column_fraction_outside_range(
    model,
    column_name,
    min_value,
    max_value,
    max_fraction_below=0.0,
    max_fraction_above=0.0,
    row_condition=None
) %}

{# Check that the fraction of non-null values falling below min_value and the fraction
falling above max_value each stay within an acceptable bound. Unlike
dbt_expectations.expect_column_values_to_be_between with error_if, which counts absolute
rows and therefore drifts as a table gains partitions, this expresses tolerance as a
share of the (optionally row_condition-filtered) population. When row_condition selects
no rows the test passes, so per-year invocations for years absent from the data do not
register as failures. #}

{% if max_fraction_below < 0.0 or max_fraction_below > 1.0 %}
    {{ exceptions.raise_compiler_error("max_fraction_below must be between 0.0 and 1.0, got: " ~ max_fraction_below) }}
{% endif %}
{% if max_fraction_above < 0.0 or max_fraction_above > 1.0 %}
    {{ exceptions.raise_compiler_error("max_fraction_above must be between 0.0 and 1.0, got: " ~ max_fraction_above) }}
{% endif %}

WITH counts AS (
    SELECT
        COUNT({{ column_name }}) AS n_total,
        COUNT(*) FILTER (WHERE {{ column_name }} < {{ min_value }}) AS n_below,
        COUNT(*) FILTER (WHERE {{ column_name }} > {{ max_value }}) AS n_above
    FROM {{ model }}
    {%- if row_condition %}
    WHERE {{ row_condition }}
    {%- endif %}
)
SELECT
    n_total,
    n_below,
    n_above,
    n_below::FLOAT / NULLIF(n_total, 0) AS fraction_below,
    n_above::FLOAT / NULLIF(n_total, 0) AS fraction_above
FROM counts
WHERE n_total > 0
    AND (
        n_below::FLOAT / n_total > {{ max_fraction_below }}
        OR n_above::FLOAT / n_total > {{ max_fraction_above }}
    )

{% endtest %}
