{% test expect_fraction_missing_foreign_key(
    model,
    fk_column_names,
    pk_table_name,
    pk_column_names,
    max_fraction=0.0,
    distinct_keys=false,
    row_condition=None,
    parent_row_condition=None
) %}

{# A tolerance-based cousin of ``foreign_key``: rather than failing on any child key
that is missing from the parent, fail only if the *fraction* of child keys missing
from the parent exceeds max_fraction. This suits relationships that hold for nearly
all rows but that filers occasionally get wrong, and it returns a one-row summary
instead of every offending row (which can be enormous for the FERC EQR tables).

As with ``foreign_key``, child rows where any key column is NULL are skipped and keys
are compared as text. If distinct_keys is true, the fraction is of distinct key
tuples rather than of rows. row_condition filters the child rows and
parent_row_condition filters the parent rows (both are SQL boolean expressions; the
parent condition may only reference parent columns). #}

{% if fk_column_names | length != pk_column_names | length %}
    {{ exceptions.raise_compiler_error(
        "expect_fraction_missing_foreign_key requires the same number of child and "
        ~ "parent key columns. child (" ~ (fk_column_names | length) ~ "): ["
        ~ (fk_column_names | join(", ")) ~ "]; parent (" ~ (pk_column_names | length)
        ~ "): [" ~ (pk_column_names | join(", ")) ~ "]"
    ) }}
{% endif %}
{% if max_fraction < 0.0 or max_fraction > 1.0 %}
    {{ exceptions.raise_compiler_error("max_fraction must be between 0.0 and 1.0, got: " ~ max_fraction) }}
{% endif %}

WITH child_keys AS (
    SELECT {% if distinct_keys %}DISTINCT{% endif %}
        {% for column_name in fk_column_names %}
        CAST({{ column_name }} AS VARCHAR) AS key_{{ loop.index }}{% if not loop.last %},{% endif %}
        {% endfor %}
    FROM {{ model }}
    WHERE
        {% for column_name in fk_column_names %}
        {{ column_name }} IS NOT NULL AND
        {% endfor %}
        {% if row_condition %}({{ row_condition }}){% else %}TRUE{% endif %}
),
parent_keys AS (
    SELECT DISTINCT
        {% for column_name in pk_column_names %}
        CAST({{ column_name }} AS VARCHAR) AS key_{{ loop.index }}{% if not loop.last %},{% endif %}
        {% endfor %}
    FROM {{ pk_table_name }}
    {% if parent_row_condition %}
    WHERE {{ parent_row_condition }}
    {% endif %}
),
counts AS (
    SELECT
        COUNT(*) AS n_total,
        COUNT(*) FILTER (WHERE parent_keys.key_1 IS NULL) AS n_missing
    FROM child_keys
    LEFT JOIN parent_keys
    ON
        {% for column_name in fk_column_names %}
        child_keys.key_{{ loop.index }} = parent_keys.key_{{ loop.index }}
        {% if not loop.last %} AND {% endif %}
        {% endfor %}
)

SELECT
    n_total,
    n_missing,
    n_missing::FLOAT / n_total AS fraction_missing,
    {{ max_fraction }} AS max_fraction
FROM counts
WHERE n_total > 0 AND n_missing::FLOAT / n_total > {{ max_fraction }}

{% endtest %}
