{% test check_row_counts_per_partition(model, table_name, partition_column) %}

{% if target.name != 'etl-full' %}
  -- Skip this test (default PASS) when not running in etl-full mode
  SELECT
    '{{ table_name }}' as table_name,
    'SKIPPED' as expected_partition,
    'SKIPPED' as observed_partition,
    NULL as expected_count,
    NULL as observed_count,
    NULL as diff_relative_to_expected
  WHERE FALSE
{% else %}
  -- Run the real test logic
  -- note 2025-03-28: logic is in a macro so that we can test it.
  {{ row_counts_per_partition(model, table_name, partition_column) }}
{% endif %}

{% endtest %}
