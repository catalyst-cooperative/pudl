{% test expect_columns_ratio(model, numerator_column, denominator_column, min_ratio=None, max_ratio=None, error_threshold=0) %}
  {% if min_ratio is none and max_ratio is none %}
    {{ exceptions.raise_compiler_error("expect_columns_ratio: must provide at least one of min_ratio or max_ratio") }}
  {% endif %}

  {% set column_list = adapter.get_columns_in_relation(model) | map(attribute="name") | list %}

  WITH ratio_check AS (
    SELECT
      *,
      CASE
        WHEN {{ denominator_column }} IS NULL OR {{ denominator_column }} = 0
        THEN NULL
        ELSE {{ numerator_column }} / {{ denominator_column }}
      END AS calculated_ratio
    FROM {{ model }}
  ),

  failures AS (
    SELECT *
    FROM ratio_check
    WHERE calculated_ratio IS NOT NULL
      AND (
        {% if min_ratio is not none %}
          calculated_ratio < {{ min_ratio }}
        {% endif %}
        {% if min_ratio is not none and max_ratio is not none %}
          OR
        {% endif %}
        {% if max_ratio is not none %}
          calculated_ratio > {{ max_ratio }}
        {% endif %}
      )
),

  failure_summary AS (
    SELECT
      COUNT(*) as num_failures,
      ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM ratio_check WHERE calculated_ratio IS NOT NULL), 2) as pct_failures
    FROM failures
  )

  SELECT
    f.*,
    fs.num_failures,
    fs.pct_failures,
    '{{ numerator_column }} / {{ denominator_column }}' as ratio_description
    {% if min_ratio is not none %}
      , '{{ min_ratio }}' as min_ratio_threshold
    {% endif %}
    {% if max_ratio is not none %}
      , '{{ max_ratio }}' as max_ratio_threshold
    {% endif %}
  FROM failures f
  CROSS JOIN failure_summary fs
  WHERE fs.num_failures > {{ error_threshold }}
{% endtest %}
