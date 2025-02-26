{% macro no_null_cols(model) %}
    {% set columns = dbt_utils.get_columns_in_relation(model) %}
    {% set null_columns = [] %}

    {% for column in columns %}
        {% set column_name = column.name %}

        -- Use the existing not_all_null test for each column
        {% if execute_test('not_all_null', model=model, column_name=column_name) %}
            -- If the test fails (all values are null), add the column to the null_columns list
            {% do null_columns.append(column_name) %}
        {% endif %}
    {% endfor %}

    {% if null_columns | length > 0 %}
        -- Return the list of columns with all null values (test fails)
        SELECT 
            '{{ null_columns | join(", ") }}' AS column_with_all_nulls
    {% else %}
        -- If no columns with all nulls, return no rows, meaning the test passes
        {% do return() %}
    {% endif %}
{% endmacro %}
