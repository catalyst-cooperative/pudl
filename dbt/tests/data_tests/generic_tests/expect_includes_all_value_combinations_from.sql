{% test expect_includes_all_value_combinations_from(model, compare_table_name, compare_cols) %}

WITH
    compare_table AS (
        SELECT DISTINCT
            {{ ", ".join(compare_cols) }}
        FROM {{ source('pudl', compare_table_name) }}
    ),
    model_table AS (
        SELECT DISTINCT
            {{ ", ".join(compare_cols) }}
        FROM {{ model }}
    )

SELECT * from compare_table
EXCEPT
SELECT * from model_table

{% endtest %}
