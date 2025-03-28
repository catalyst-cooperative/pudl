{% test check_row_counts_per_partition(model, table_name, partition_column) %}

-- note 2025-03-28: logic is in a macro so that we can test it.
{{ row_counts_per_partition(model, table_name, partition_column) }}

{% endtest %}
