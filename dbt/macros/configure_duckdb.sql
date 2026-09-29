{#
    Apply DuckDB resource limits from environment variables.

    Runs as an ``on-run-start`` hook (see dbt_project.yml). A setting is only changed
    if its environment variable is set; otherwise DuckDB's own default applies. That
    is why this is a hook and not the ``settings`` block of profiles.yml, which can't
    express "leave the default alone".

    Local development and production builds use different machines, so all of the
    limits can be overridden without editing any files:

        PUDL_DBT_MEMORY_LIMIT  DuckDB ``memory_limit``, e.g. ``16GB``
        PUDL_DBT_THREADS       DuckDB worker ``threads`` (not dbt's own ``threads``)
        PUDL_DBT_TEMP_DIR      ``temp_directory``, where DuckDB spills to disk

    The ``etl-full-large`` target validates the tables marked ``large``. It uses the
    ``PUDL_DBT_LARGE_*`` version of each variable, falling back to the general one.
    ``pudl.validate.dbt.duckdb_settings`` mirrors this logic so that failing queries
    are re-run under the same limits. Keep the two in sync.
#}
{% macro configure_duckdb() %}
    {% if execute %}
        {% set is_large = target.name == "etl-full-large" %}
        {% set settings = {
            "memory_limit": "MEMORY_LIMIT",
            "threads": "THREADS",
            "temp_directory": "TEMP_DIR",
        } %}
        {% for setting, suffix in settings.items() %}
            {% set general_value = env_var("PUDL_DBT_" ~ suffix, "") %}
            {% set value = env_var("PUDL_DBT_LARGE_" ~ suffix, general_value) if is_large else general_value %}
            {% if value %}
                {% do log("Setting DuckDB " ~ setting ~ " = " ~ value, info=True) %}
                {% do run_query("SET " ~ setting ~ " = '" ~ value ~ "'") %}
            {% endif %}
        {% endfor %}
        {# None of our tests depend on row order, and preserving it costs memory. #}
        {% do run_query("SET preserve_insertion_order = false") %}
    {% endif %}
{% endmacro %}
