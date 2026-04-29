{% test foreign_key(model, fk_column_names, pk_table_name, pk_column_names) %}
-- As of 2026-04 we couldn't find a library that provided composite key foreign key checks.
--
-- So we're rolling our own.
--
-- SQLite ignores FKs if any of the key values in the child row are NULL, so we copy that behavior here.
-- We grab the *whole* child row so we can see it in the failure.
SELECT
    child_rows.*,
    'missing_parent_key' AS failure_type
FROM {{ model }} AS child_rows
ANTI JOIN {{ pk_table_name }} AS parent_rows
ON
    {% for fk_column_name, pk_column_name in zip(fk_column_names, pk_column_names) %}
    child_rows.{{ fk_column_name }} = parent_rows.{{ pk_column_name }}
    {% if not loop.last %} AND {% endif %}
    {% endfor %}
WHERE
    (
        {% for column_name in fk_column_names %}
        child_rows.{{ column_name }} IS NOT NULL
        {% if not loop.last %} AND {% endif %}
        {% endfor %}
    )

{% endtest %}
