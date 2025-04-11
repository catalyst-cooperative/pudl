{% macro debug_quantile_constraints(table, test) %}

{% if table.startswith("validate") %}
{% set fq_table = "model.pudl_dbt." ~ table %}
{% else %}
{% set fq_table = "source.pudl_dbt.pudl." ~ table %}
{% endif %}

{% for node in graph.nodes.values() if node.resource_type == "test" and fq_table in node.depends_on.nodes and (node.name == test or node.test_metadata.name == test) %}
{% set kwargs = node.test_metadata.kwargs %}
{% if table.startswith("validate") %}
{% set model = ref(table) %}
{% else %}
{% set model = get_where_subquery(source('pudl', table)) %}
{% endif %}
{% set column_name = kwargs.column_name %}
{% set weight_column = kwargs.weight_column %}
{% set row_condition = kwargs.row_condition %}

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

{% do log(fq_table, info=True) %}
{% do log(column_name, info=True) %}
{% do log(node.test_metadata.name, info=True) %}
{% do log(row_condition, info=True) %}

{% set results = run_query(expression) %}

{% do log(" quantile |     value |       min |       max", info=True) %}
{% for row in results.rows %}
{% do log("%9.9s | %9.9s | %9.9s | %9.9s" % (row[0], row[1], row[2], row[3]), info=True) %}
{% endfor %}

{% endfor %}

{% endmacro %}
