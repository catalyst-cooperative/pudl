{% test subcomponents_sum_to_total(
    model,
    group_by_columns,
    categorical_column,
    value_column,
    subcomponents_list,
    total_label,
    tolerance=0.01,
    row_condition=None,
    negative_subcomponents_list=None
) %}

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
        {{ categorical_column }},
        SUM({{ value_column }}) AS total
    FROM filtered
    GROUP BY {{ group_by_columns | join(', ') }}, {{ categorical_column }}
),

summary AS (
    SELECT
        {{ group_by_columns | join(', ') }},

        -- Calculate weighted sum of positive and negative subcomponents
        SUM(
            CASE
                WHEN {{ categorical_column }} IN (
                    {%- for subcomp in subcomponents_list %}
                    '{{ subcomp }}'{% if not loop.last %}, {% endif %}
                    {%- endfor %}
                ) THEN total
                {% if negative_subcomponents_list is not none and negative_subcomponents_list | length > 0 %}
                WHEN {{ categorical_column }} IN (
                    {%- for neg_subcomp in negative_subcomponents_list %}
                    '{{ neg_subcomp }}'{% if not loop.last %}, {% endif %}
                    {%- endfor %}
                ) THEN -1 * total
                {% endif %}
            END
        ) AS subcomponents_sum,

        MAX(CASE WHEN {{ categorical_column }} = '{{ total_label }}' THEN total END) AS grand_total,

        ABS(
            SUM(
                CASE
                    WHEN {{ categorical_column }} IN (
                        {%- for subcomp in subcomponents_list %}
                        '{{ subcomp }}'{% if not loop.last %}, {% endif %}
                        {%- endfor %}
                    ) THEN total
                    {% if negative_subcomponents_list is not none and negative_subcomponents_list | length > 0 %}
                    WHEN {{ categorical_column }} IN (
                        {%- for neg_subcomp in negative_subcomponents_list %}
                        '{{ neg_subcomp }}'{% if not loop.last %}, {% endif %}
                        {%- endfor %}
                    ) THEN -1 * total
                    {% endif %}
                END
            ) -
            MAX(CASE WHEN {{ categorical_column }} = '{{ total_label }}' THEN total END)
        ) AS absolute_diff,

        ROUND(
            ABS(
                SUM(
                    CASE
                        WHEN {{ categorical_column }} IN (
                            {%- for subcomp in subcomponents_list %}
                            '{{ subcomp }}'{% if not loop.last %}, {% endif %}
                            {%- endfor %}
                        ) THEN total
                        {% if negative_subcomponents_list is not none and negative_subcomponents_list | length > 0 %}
                        WHEN {{ categorical_column }} IN (
                            {%- for neg_subcomp in negative_subcomponents_list %}
                            '{{ neg_subcomp }}'{% if not loop.last %}, {% endif %}
                            {%- endfor %}
                        ) THEN -1 * total
                        {% endif %}
                    END
                ) -
                MAX(CASE WHEN {{ categorical_column }} = '{{ total_label }}' THEN total END)
            ) / NULLIF(MAX(CASE WHEN {{ categorical_column }} = '{{ total_label }}' THEN total END), 0) * 100,
            2
        ) AS pct_diff

    FROM grouped
    GROUP BY {{ group_by_columns | join(', ') }}
)

SELECT *
FROM summary
WHERE ABS(subcomponents_sum - grand_total) > {{ tolerance }}

{% endtest %}
