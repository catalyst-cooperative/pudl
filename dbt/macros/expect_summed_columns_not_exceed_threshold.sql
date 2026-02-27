{% test expect_summed_columns_not_exceed_threshold(model, column_a, column_b, threshold_column, multiplier=1.0, error_threshold=0) %}
  {% if threshold_column is none %}
    {{ exceptions.raise_compiler_error("expect_summed_columns_not_exceed_threshold: threshold_column parameter is required") }}
  {% endif %}

  {% set column_list = adapter.get_columns_in_relation(model) | map(attribute="name") | list %}

  WITH sum_check AS (
    SELECT
      *,
      ({{ column_a }} + {{ column_b }}) AS column_sum,
      ({{ threshold_column }} * {{ multiplier }}) AS max_threshold
    FROM {{ model }}
  ),

  failures AS (
    SELECT *
    FROM sum_check
    WHERE column_sum IS NOT NULL
      AND max_threshold IS NOT NULL
      AND column_sum > max_threshold
  ),

  failure_summary AS (
    SELECT
      COUNT(*) as num_failures,
      ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM sum_check WHERE column_sum IS NOT NULL AND max_threshold IS NOT NULL), 2) as pct_failures,
      ROUND(MAX(column_sum), 2) as max_observed_sum,
      ROUND(AVG(column_sum), 2) as avg_observed_sum,
      ROUND(MAX(column_sum - max_threshold), 2) as max_overage,
      ROUND(AVG(column_sum - max_threshold), 2) as avg_overage
    FROM failures
  )

  SELECT
    f.*,
    fs.num_failures,
    fs.pct_failures,
    fs.max_observed_sum,
    fs.avg_observed_sum,
    fs.max_overage,
    fs.avg_overage,
    '{{ column_a }} + {{ column_b }}' as sum_description,
    '{{ threshold_column }} * {{ multiplier }}' as threshold_description
  FROM failures f
  CROSS JOIN failure_summary fs
  WHERE fs.num_failures > {{ error_threshold }}
{% endtest %}
