{% test compare_all_same_col_values(model, compare_df_name, compare_cols) %}

WITH
    compare_df AS (
        SELECT DISTINCT
            {{ ", ".join(compare_cols) }}
        FROM {{ source('pudl', compare_df_name )}}
    ),
    model_df AS (
        SELECT DISTINCT
            {{ ", ".join(compare_cols) }}
        FROM {{ model }}
    )

SELECT * from compare_df
EXCEPT
SELECT * from model_df

{% endtest %}
