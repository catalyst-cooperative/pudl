-- Each case should (or should not) return failure rows. The final SELECT reports any
-- case that disagrees with its expectation, so it returns no rows if the test works.
-- (The partition_column path runs a query against the real table when compiled, so it
-- can't be exercised against these in-memory test tables.)
WITH key_table AS (
    SELECT * FROM (VALUES
        ('q1', 'a', 1, 'x'),
        ('q1', 'a', 2, 'x'),
        ('q1', 'a', 3, 'x'),
        ('q1', 'b', 4, 'y'),
        ('q2', 'a', 5, 'z'),
        ('q2', 'c', 6, 'w'),
        ('q2', 'c', 7, 'w'),
        ('q2', NULL, 8, 'n'),
        ('q2', NULL, 9, 'n')
    ) AS t(quarter, id, row_id, payload)
),
payload_table AS (
    SELECT quarter, id, payload FROM key_table
),
cases AS (
    -- The 8 rows with non-null keys include 3 duplicates (a x3 -> 2, c x2 -> 1) and
    -- the rows with NULL ids are skipped: rate 3/7. Fails when strict.
    SELECT 'strict_fail' AS case_name, 2 AS expected_rows, COUNT(*) AS n_rows
    FROM ({{ test_expect_unique_key_fraction('key_table', ['quarter', 'id']) }})
    UNION ALL
    SELECT 'tolerance_fail', 2, COUNT(*)
    FROM ({{ test_expect_unique_key_fraction(
        'key_table', ['quarter', 'id'], max_fraction=0.4) }})
    UNION ALL
    SELECT 'tolerance_pass', 0, COUNT(*)
    FROM ({{ test_expect_unique_key_fraction(
        'key_table', ['quarter', 'id'], max_fraction=0.5) }})
    UNION ALL
    -- A unique key passes.
    SELECT 'unique_pass', 0, COUNT(*)
    FROM ({{ test_expect_unique_key_fraction('key_table', ['row_id']) }})
    UNION ALL
    -- row_condition restricts the rows: only quarter q2 with a non-null id leaves one
    -- duplicate among 3 rows.
    SELECT 'row_condition_fail', 1, COUNT(*)
    FROM ({{ test_expect_unique_key_fraction(
        'key_table', ['quarter', 'id'], row_condition="quarter = 'q2'") }})
    UNION ALL
    SELECT 'row_condition_pass', 0, COUNT(*)
    FROM ({{ test_expect_unique_key_fraction(
        'key_table', ['quarter', 'id'], row_condition="quarter = 'q1' AND id = 'b'") }})
    UNION ALL
    -- Whole-row mode: rows 1-3 share (q1, a, x) only if row_id is ignored, but
    -- row_id differs, so there are no exact duplicates in key_table...
    SELECT 'whole_row_unique_pass', 0, COUNT(*)
    FROM ({{ test_expect_unique_key_fraction('key_table', ['*']) }})
    UNION ALL
    -- ... but dropping row_id exposes them, including the NULL-id rows, which compare
    -- as equal in whole-row mode: duplicates are (q1,a,x) x2 extra, (q2,c,w) x1 and
    -- (q2,NULL,n) x1, out of 9 rows.
    SELECT 'whole_row_dups_fail', 3, COUNT(*)
    FROM ({{ test_expect_unique_key_fraction('payload_table', ['*']) }})
    UNION ALL
    -- sample_size caps the returned rows.
    SELECT 'sample_size', 1, COUNT(*)
    FROM ({{ test_expect_unique_key_fraction(
        'key_table', ['quarter', 'id'], sample_size=1) }})
),
-- The most-repeated key is reported first, with the whole-table duplicate rate.
report AS (
    SELECT 'reports_rate' AS case_name, 1 AS expected_rows,
        COUNT(*) FILTER (
            WHERE id = 'a' AND n_rows = 3 AND n_duplicate_rows = 3 AND n_checked = 7
        ) AS n_rows
    FROM ({{ test_expect_unique_key_fraction(
        'key_table', ['quarter', 'id'], sample_size=1) }})
)

SELECT * FROM cases WHERE n_rows != expected_rows
UNION ALL
SELECT * FROM report WHERE n_rows != expected_rows
