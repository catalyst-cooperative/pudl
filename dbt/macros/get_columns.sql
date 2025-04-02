{% macro get_columns(model, table_name=None) %}
    {% if table_name is none %}
        {% set table_name = model.identifier %}
    {% endif %}
    
    {% set sql %}
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = '{{ table_name }}'
    {% endset %}
    
    {% set results = run_query(sql) %}
    
    {% if execute %}
        {% set columns = results.columns[0].values() %}
    {% else %}
        {% set columns = [] %}
    {% endif %}
    
    {{ return(columns) }}
{% endmacro %}