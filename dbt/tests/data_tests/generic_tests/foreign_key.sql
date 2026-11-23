-- As of 2026-04 we couldn't find a library that provided composite key foreign key checks.
--
-- So we're rolling our own.
--
-- SQLite ignores FKs if any of the key values in the child row are NULL, so we copy that behavior here.
-- If NULL keys are themselves a problem, test for them separately (e.g. with
-- dbt_utils.not_null_proportion) so they don't hide or inflate the FK failure rate.
--
-- By default the test is strict: any child row whose key is missing from the parent
-- fails. Set max_fraction to tolerate a share of failing rows, for relationships that
-- hold for almost all rows but not all. Either way the output is a sample of the failing
-- child rows (at most sample_size of them, so a badly broken FK doesn't return millions
-- of rows) along with the number of failures and the failure rate.
--
-- row_condition restricts which child rows are checked, and parent_row_condition
-- restricts which parent rows count as present. Both are SQL boolean expressions over
-- that table's own columns.
{% test foreign_key(
    model,
    fk_column_names,
    pk_table_name,
    pk_column_names,
    max_fraction=0.0,
    row_condition=None,
    parent_row_condition=None,
    sample_size=20
) %}

-- NOTE 2026-04-30: If we don't do this check, zip() will happily let us try to match one child column to two parent columns.
{% if fk_column_names | length != pk_column_names | length %}
    {{ exceptions.raise_compiler_error(
        "foreign_key requires same number of child and parent key columns. "
        ~ "child ("
        ~ (fk_column_names | length)
        ~ "): ["
        ~ (fk_column_names | join(", "))
        ~ "]; parent ("
        ~ (pk_column_names | length)
        ~ "): ["
        ~ (pk_column_names | join(", "))
        ~ "]"
    ) }}
{% endif %}
{% if max_fraction < 0.0 or max_fraction > 1.0 %}
    {{ exceptions.raise_compiler_error("max_fraction must be between 0.0 and 1.0, got: " ~ max_fraction) }}
{% endif %}

WITH child_rows AS (
    SELECT *
    FROM {{ model }}
    WHERE
        (
            {% for column_name in fk_column_names %}
            {{ column_name }} IS NOT NULL
            {% if not loop.last %} AND {% endif %}
            {% endfor %}
        )
        {% if row_condition %}
        AND ({{ row_condition }})
        {% endif %}
),
parent_rows AS (
    SELECT *
    FROM {{ pk_table_name }}
    {% if parent_row_condition %}
    WHERE {{ parent_row_condition }}
    {% endif %}
),
-- We grab the *whole* child row so we can see it in the failure.
failing_rows AS (
    SELECT child_rows.*
    FROM child_rows
    ANTI JOIN parent_rows
    -- Always compare the key columns as text. A comparison between mismatched types
    -- (e.g. integer child key vs. string parent key) throws a conversion error, and
    -- then the test can't report anything at all. This resulted in a handful of
    -- FK checks being silently skipped historically. Whether the FK column types
    -- agree is a schema question that belongs with the PUDL metadata, not in this
    -- data test. See https://github.com/catalyst-cooperative/pudl/pull/5554
    ON
        {% for fk_column_name, pk_column_name in zip(fk_column_names, pk_column_names) %}
        CAST(child_rows.{{ fk_column_name }} AS VARCHAR) = CAST(parent_rows.{{ pk_column_name }} AS VARCHAR)
        {% if not loop.last %} AND {% endif %}
        {% endfor %}
),
counts AS (
    SELECT
        (SELECT COUNT(*) FROM child_rows) AS n_checked,
        (SELECT COUNT(*) FROM failing_rows) AS n_failures
)

SELECT
    {% for column_name in fk_column_names %}
    failing_rows.{{ column_name }},
    {% endfor %}
    'missing_parent_key' AS failure_type,
    failing_rows.* EXCLUDE (
        {% for column_name in fk_column_names %}
        {{ column_name }}{% if not loop.last %}, {% endif %}
        {% endfor %}
    ),
    counts.n_failures,
    counts.n_checked,
    counts.n_failures::DOUBLE / counts.n_checked AS failure_rate,
    {{ max_fraction }} AS max_fraction
FROM failing_rows
CROSS JOIN counts
WHERE counts.n_failures::DOUBLE / counts.n_checked > {{ max_fraction }}
LIMIT {{ sample_size }}

{% endtest %}
