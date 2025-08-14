{% test check_row_counts_per_partition(model, table_name, partition_expr) %}

{{ config(
  enabled=(target.name == 'etl-full')
) }}

-- note 2025-03-28: logic is in a macro so that we can test it.
{{ row_counts_per_partition(model, table_name, partition_expr) }}

{% endtest %}
