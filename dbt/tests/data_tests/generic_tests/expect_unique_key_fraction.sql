{% test expect_unique_key_fraction(
    model,
    key_columns,
    max_fraction=0.0,
    row_condition=None,
    partition_column=None,
    sample_size=20
) %}

{# Check that a (pseudo) primary key is unique, allowing a fraction of duplicate rows.
Useful for tables with no enforced primary key, to set a baseline for future improvement.

A row is a duplicate if an earlier row has the same key, so a key that appears n times
contributes n - 1 duplicate rows. The test fails if duplicate rows are more than
max_fraction of the checked rows. As with foreign_key, rows where any key column is NULL
are skipped. Pass key_columns=['*'] to instead check for exact duplicate rows, treating
NULLs as equal to each other. The output is a sample of the most-repeated keys (at most
sample_size) with their row counts, along with the number of duplicate rows and the
duplicate rate.

If partition_column is given (it should also be one of key_columns), duplicates are
counted separately within each distinct value of it, using one query per value that is
run one after another. That's equivalent, since keys with different partition values
can't collide, but it guarantees that only one partition's keys are held in memory at a
time, which matters for huge partitioned tables like the FERC EQR transactions (billions
of rows). It takes one pass per partition, so it's slower, and it can't be exercised
against the in-memory tables of a macro unit test. #}

{% set whole_row = key_columns == ['*'] %}
{% if max_fraction < 0.0 or max_fraction > 1.0 %}
    {{ exceptions.raise_compiler_error("max_fraction must be between 0.0 and 1.0, got: " ~ max_fraction) }}
{% endif %}
{% if partition_column is not none and not whole_row and partition_column not in key_columns %}
    {{ exceptions.raise_compiler_error("partition_column must be one of key_columns") }}
{% endif %}

{% set checked_filter = key_checked_filter(key_columns, row_condition) %}

{% if partition_column is none %}

WITH duplicate_keys AS (
    {{ duplicate_keys_query(model, key_columns, checked_filter) }}
),
counts AS (
    SELECT
        (SELECT COUNT(*) FROM {{ model }} WHERE {{ checked_filter }}) AS n_checked,
        (SELECT COALESCE(SUM(n_rows - 1), 0) FROM duplicate_keys) AS n_duplicate_rows
)

SELECT
    duplicate_keys.*,
    counts.n_duplicate_rows,
    counts.n_checked,
    counts.n_duplicate_rows::DOUBLE / counts.n_checked AS duplicate_rate,
    {{ max_fraction }} AS max_fraction
FROM duplicate_keys
CROSS JOIN counts
WHERE counts.n_duplicate_rows::DOUBLE / counts.n_checked > {{ max_fraction }}
ORDER BY duplicate_keys.n_rows DESC
LIMIT {{ sample_size }}

{% else %}

{# Count duplicates one partition at a time with run_query, so partitions are
guaranteed to be processed sequentially, then return a one-row summary. #}
{% set n_checked = namespace(value=0) %}
{% set n_duplicate_rows = namespace(value=0) %}
{% set sample = namespace(lines=[]) %}
{% if execute %}
    {% set partitions = run_query(
        "SELECT DISTINCT " ~ partition_column ~ " FROM " ~ model ~ " WHERE " ~ checked_filter ~ " ORDER BY 1"
    ).columns[0].values() %}
    {% for partition in partitions %}
        {% set extra %}AND {{ partition_column }} = '{{ partition | replace("'", "''") }}'{% endset %}
        {% set n_checked.value = n_checked.value + run_query(
            "SELECT COUNT(*) FROM " ~ model ~ " WHERE " ~ checked_filter ~ " " ~ extra
        ).columns[0].values()[0] %}
        {% set dups = run_query(
            "SELECT COALESCE(SUM(n_rows - 1), 0) FROM (" ~ duplicate_keys_query(model, key_columns, checked_filter, extra) ~ ")"
        ).columns[0].values()[0] %}
        {% set n_duplicate_rows.value = n_duplicate_rows.value + dups %}
        {% if dups > 0 and sample.lines | length < sample_size %}
            {% set rows = run_query(
                "SELECT * FROM (" ~ duplicate_keys_query(model, key_columns, checked_filter, extra) ~ ") ORDER BY n_rows DESC LIMIT "
                ~ (sample_size - (sample.lines | length))
            ) %}
            {% for row in rows.rows %}
                {% set _ = sample.lines.append(row | join(" | ")) %}
            {% endfor %}
        {% endif %}
    {% endfor %}
{% endif %}

SELECT
    {{ n_duplicate_rows.value }} AS n_duplicate_rows,
    {{ n_checked.value }} AS n_checked,
    {{ n_duplicate_rows.value }}::DOUBLE / NULLIF({{ n_checked.value }}, 0) AS duplicate_rate,
    {{ max_fraction }} AS max_fraction,
    '{{ (sample.lines | join(" ;; ")) | replace("'", "''") }}' AS sample_duplicate_keys
WHERE {{ n_duplicate_rows.value }}::DOUBLE / NULLIF({{ n_checked.value }}, 0) > {{ max_fraction }}

{% endif %}

{% endtest %}
