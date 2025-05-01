{% macro debug_quantile_constraints(table, test) %}

{# Get the fully-qualified path to the table: #}
{% if table.startswith("validate") %}
{% set fq_table = "model.pudl_dbt." ~ table %}
{% else %}
{% set fq_table = "source.pudl_dbt.pudl." ~ table %}
{% endif %}

{# Walk the dbt graph and find the nodes that are:
    - tests
    - that depend on our table
    - with the name of the test that failed
   Then grab some useful attributes we'll need later.
#}
{% for node in graph.nodes.values() if node.resource_type == "test" and fq_table in node.depends_on.nodes and (node.name == test or node.test_metadata.name == test) %}
{% set kwargs = node.test_metadata.kwargs %}
{% if table.startswith("validate") %}
        {# these expressions are actually stored as jinja strings in test_metadata.model
        but jinja doesn't have an eval() function so we have to use this if statement
        instead #}
    {% set model = ref(table) %}
    {% else %}
    {% set model = get_where_subquery(source('pudl', table)) %}
{% endif %}
{% set column_name = kwargs.column_name %}
{% set weight_column = kwargs.weight_column %}
{% set row_condition = kwargs.row_condition %}

{# Construct a SQL query (but do not run it yet) to fetch:
    - for each constraint,
        - the quantile (from the test yaml)
        - the computed quantile value (based on the data)
        - the lower bound (from the test yaml)
        - the upper bound (from the test yaml)
#}
{% set expression %}
with
{% for constraint in kwargs.constraints %}
    Quantile_{{ loop.index }} as
    (
    {% if kwargs.weight_column %}{{ weighted_quantile(model, column_name, weight_column, constraint.quantile, row_condition) }}
    {% else %}select percentile_cont({{ constraint.quantile }}) within group (order by {{ column_name }}) as interpolated_value from {{ model }} {% if row_condition %}where {{ row_condition }}{% endif %}
    {% endif %}
    ){% if not loop.last %},{% endif %}
{% endfor %}
{% for constraint in kwargs.constraints %}
select {{ constraint.quantile }} as quantile, interpolated_value, {{ constraint.min_value | default("NULL") }} as min_value, {{ constraint.max_value | default("NULL") }} as max_value from Quantile_{{ loop.index }}
{% if not loop.last %}UNION ALL{% endif %}
{% endfor %}
{% endset %}

{# Print out some convenience info to:
    - help you find the test yaml, should you need to edit it (table/column/row_condition)
    - tell you if this test was already flagged as being suspicious (description)
#}
{% do log("table: " ~ fq_table, info=True) %}
{% do log("test: " ~ node.test_metadata.name, info=True) %}
{% do log("column: " ~ column_name, info=True) %}
{% do log("row_condition: " ~ row_condition, info=True) %}
{% do log("weight column: " ~ weight_column, info=True) %}
{% do log("description: " ~ node.description, info=True) %}

{# Run the SQL query and collect the results #}
{% set results = run_query(expression) %}

{# For each constraint, print out:
    - the quantile (from the test yaml)
    - the computed quantile value (based on the data)
    - the lower bound "min" (from the test yaml)
    - the upper bound "max" (from the test yaml)
#}
{% do log(" quantile |     value |       min |       max", info=True) %}
{% for row in results.rows %}
    {# we use %s here because it knows how to handle nulls, but that means any value more
    than 1e9 will get silently truncated. this will be obvious as soon as you look at the
    test yaml though, and hopefully you will find this comment and increase these field
    widths accordingly. #}
{% do log("%9.9s | %9.9s | %9.9s | %9.9s" % (row[0], row[1], row[2], row[3]), info=True) %}
{% endfor %}

{% endfor %}

{% endmacro %}
