{#
    Look up which dbt source a table belongs to, so that macros and tests don't need to
    hard-code source names (``pudl``, ``ferceqr``) and can refer to any table by name.

    Table names are unique across sources. The lookup walks the dbt graph, which is only
    populated at execution time. During parsing there is nothing to resolve, and the
    macros return nothing since parsed SQL is never executed.
#}
{% macro find_source_node(table_name) %}
    {% set matches = graph.sources.values() | selectattr("name", "equalto", table_name) | list %}
    {% if matches | length != 1 %}
        {{ exceptions.raise_compiler_error(
            "Expected exactly one dbt source named " ~ table_name ~ ", found " ~ (matches | length)
        ) }}
    {% endif %}
    {{ return(matches[0]) }}
{% endmacro %}

{% macro source_for_table(table_name) %}
    {% if execute %}
        {{ return(source(find_source_node(table_name).source_name, table_name)) }}
    {% endif %}
{% endmacro %}
