{# Units that measure energy (``$/MWH``, ``CENTS/KWH``, ...) and power or capacity
(``$/MW``, ``$/KW-MO``, ``$/MW-DAY``, ...), as case-insensitive SQL LIKE patterns. The
defaults follow the allowable rate units in Appendix D of the FERC EQR data dictionary.
Neutral units, such as ``FLAT RATE``, don't specify a dimension and are allowed by
default alongside whichever dimension a test expects. #}
{% macro energy_unit_patterns() %}
    {{ return(['%KWH', '%MWH']) }}
{% endmacro %}

{% macro power_unit_patterns() %}
    {{ return([
        '%-MO', '%-DAY', '%-WK', '%-YR',
        '$/KW', '$/MW', '$/KVA', '$/KV', '$/KVR', '$/RKVA',
    ]) }}
{% endmacro %}

{% macro neutral_unit_patterns() %}
    {{ return(['FLAT RATE']) }}
{% endmacro %}

{% macro unit_mismatch_check(
    model,
    unit_col,
    allowed_unit_patterns,
    max_failure_rate,
    row_condition=None
) %}

{# Logic behind the expect_energy_units and expect_power_units data tests. Among the
rows selected by ``row_condition`` (all rows if omitted) whose unit_col is not NULL,
find the fraction whose unit matches none of the ``allowed_unit_patterns``, and return
a one-row summary if it exceeds ``max_failure_rate``. The rate is pooled across
all of the selected rows, so use separate tests to give different groups of rows their
own thresholds. #}

{% if allowed_unit_patterns | length == 0 %}
    {{ exceptions.raise_compiler_error("allowed_unit_patterns must not be empty") }}
{% endif %}
{% if max_failure_rate < 0.0 or max_failure_rate > 1.0 %}
    {{ exceptions.raise_compiler_error("max_failure_rate must be between 0.0 and 1.0, got: " ~ max_failure_rate) }}
{% endif %}

WITH checked AS (
    SELECT
        NOT (
            {% for pattern in allowed_unit_patterns %}
            {{ unit_col }} ILIKE '{{ pattern | replace("'", "''") }}'
            {% if not loop.last %} OR {% endif %}
            {% endfor %}
        ) AS is_mismatch
    FROM {{ model }}
    WHERE
        {{ unit_col }} IS NOT NULL
        {% if row_condition %}
        AND ({{ row_condition }})
        {% endif %}
),
counts AS (
    SELECT
        COUNT(*) AS n_total,
        COUNT(*) FILTER (WHERE is_mismatch) AS n_mismatched
    FROM checked
)

SELECT
    n_total,
    n_mismatched,
    n_mismatched::FLOAT / n_total AS failure_rate,
    {{ max_failure_rate }} AS max_failure_rate
FROM counts
WHERE n_total > 0 AND n_mismatched::FLOAT / n_total > {{ max_failure_rate }}

{% endmacro %}
