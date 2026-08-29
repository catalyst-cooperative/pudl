{% macro subcomponent_one_label_condition(categorical_columns, label) -%}
    {#-
        Render a SQL condition matching one subcomponent or total label.

        `label` may be a single value (matched against the single categorical
        column) or a list of values matched pairwise against the list of
        categorical columns, e.g. ["opex", "total"] with columns
        ["cost_group", "cost_type"] renders (cost_group = 'opex' AND cost_type = 'total').
    -#}
    {%- set label_values = [label] if label is string else label -%}
    {%- if label_values | length != categorical_columns | length -%}
        {{ exceptions.raise_compiler_error(
            "subcomponents_sum_to_total: label " ~ label
            ~ " must have one value per categorical column: " ~ categorical_columns
        ) }}
    {%- endif -%}
    (
    {%- for column in categorical_columns -%}
        {{ column }} = '{{ label_values[loop.index0] }}'{% if not loop.last %} AND {% endif %}
    {%- endfor -%}
    )
{%- endmacro %}

{% macro subcomponent_all_labels_condition(categorical_columns, labels) -%}
    {#- Render a SQL condition matching any of the given labels. -#}
    (
    {%- for label in labels -%}
        {{ subcomponent_one_label_condition(categorical_columns, label) }}{% if not loop.last %} OR {% endif %}
    {%- endfor -%}
    )
{%- endmacro %}

{% macro subcomponents_sum_to_total_check(
    model,
    group_by_columns,
    categorical_column,
    value_column,
    total_label,
    tolerance=0.01,
    row_condition=None,
    subcomponents_list=None,
    negative_subcomponents_list=None
) %}

{#- Argument documentation lives in dbt/macros/schema.yml. -#}
{% set categorical_columns = [categorical_column] if categorical_column is string else categorical_column %}

WITH filtered AS (
    SELECT *
    FROM {{ model }}
    {% if row_condition is not none %}
    WHERE {{ row_condition }}
    {% endif %}
),

grouped AS (
    SELECT
        {{ group_by_columns | join(', ') }},
        {{ categorical_columns | join(', ') }},
        SUM({{ value_column }}) AS total
    FROM filtered
    GROUP BY {{ group_by_columns | join(', ') }}, {{ categorical_columns | join(', ') }}
),

summary AS (
    SELECT
        {{ group_by_columns | join(', ') }},

        -- Calculate weighted sum of positive and negative subcomponents (or all except total)
        SUM(
            CASE
                {% if negative_subcomponents_list is not none and negative_subcomponents_list | length > 0 %}
                -- Negative subcomponents come first because when
                -- subcomponents_list is omitted, the positive branch below
                -- matches everything except the total_label - including the
                -- negative subcomponents, which would silently be added
                -- instead of subtracted if they were matched second
                WHEN {{ subcomponent_all_labels_condition(categorical_columns, negative_subcomponents_list) }} THEN -1 * total
                {% endif %}
                {% if subcomponents_list is not none and subcomponents_list | length > 0 %}
                -- Use provided subcomponents list
                WHEN {{ subcomponent_all_labels_condition(categorical_columns, subcomponents_list) }} THEN total
                {% else %}
                -- Sum everything except the total_label
                WHEN NOT {{ subcomponent_one_label_condition(categorical_columns, total_label) }} THEN total
                {% endif %}
            END
        ) AS subcomponents_sum,

        -- Calculate totals
        MAX(CASE WHEN {{ subcomponent_one_label_condition(categorical_columns, total_label) }} THEN total END) AS grand_total,

        -- Calculate the absolute difference between the subcomponents and total
        ABS(subcomponents_sum - grand_total) AS absolute_diff,

        -- Get a percent difference between these two values
        ROUND(
            ABS(subcomponents_sum - grand_total) / NULLIF(grand_total, 0) * 100, 2
        ) AS pct_diff

    FROM grouped
    GROUP BY {{ group_by_columns | join(', ') }}
)

SELECT *
FROM summary
WHERE ABS(subcomponents_sum - grand_total) > {{ tolerance }}

{% endmacro %}

{% test subcomponents_sum_to_total(
    model,
    group_by_columns,
    categorical_column,
    value_column,
    total_label,
    tolerance=0.01,
    row_condition=None,
    subcomponents_list=None,
    negative_subcomponents_list=None
) %}

{{ subcomponents_sum_to_total_check(
    model,
    group_by_columns,
    categorical_column,
    value_column,
    total_label,
    tolerance=tolerance,
    row_condition=row_condition,
    subcomponents_list=subcomponents_list,
    negative_subcomponents_list=negative_subcomponents_list
) }}

{% endtest %}
