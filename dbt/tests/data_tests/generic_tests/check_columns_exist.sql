{% test check_columns_exist(model, table_name = None) %}
    {% if execute %}
        {% set columns = get_columns(model, table_name) %}
        {{ print("Columns found: " ~ columns) }}

        {% if columns | length == 0 %}
            -- This will fail the test and return a message
            {{ exceptions.raise_compiler_error('No columns found for model or source: ' ~ model.identifier) }}
        {% else %}
            -- Optionally log the columns for debugging
            {{ log("Columns for model or source: " ~ model.identifier ~ ": " ~ columns | join(', '), info=True) }}
        {% endif %}
    {% else %}
        -- During compilation, we can't query the database, so we set columns to an empty list
        {% set columns = [] %}
    {% endif %}
{% endtest %}
