{# Helpers for the expect_unique_key_fraction data test. #}

{% macro key_checked_filter(key_columns, row_condition=None) %}
    {#- SQL condition selecting the rows checked for duplicates: those with no NULL key
    column (or all rows, for the whole-row key ['*']), and matching row_condition. -#}
    {%- if key_columns == ['*'] -%}
    TRUE
    {%- else -%}
    {%- for column_name in key_columns -%}
    {{ column_name }} IS NOT NULL{% if not loop.last %} AND {% endif %}
    {%- endfor -%}
    {%- endif -%}
    {%- if row_condition %} AND ({{ row_condition }}){% endif -%}
{% endmacro %}

{% macro duplicate_keys_query(model, key_columns, checked_filter, extra_filter="") %}
    {#- One row per key that appears more than once, with its row count. -#}
    {%- set whole_row = key_columns == ['*'] -%}
    {%- set key_list = key_columns | join(", ") -%}
    SELECT {% if whole_row %}*{% else %}{{ key_list }}{% endif %}, COUNT(*) AS n_rows
    FROM {{ model }}
    WHERE {{ checked_filter }} {{ extra_filter }}
    GROUP BY {% if whole_row %}ALL{% else %}{{ key_list }}{% endif %}
    HAVING COUNT(*) > 1
{% endmacro %}
