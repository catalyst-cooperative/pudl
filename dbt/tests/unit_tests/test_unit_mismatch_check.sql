-- Each case should (or should not) return a failure row. The final SELECT reports any
-- case that disagrees with its expectation, so it returns no rows if the macro works.
WITH unit_table AS (
    SELECT * FROM (VALUES
        ('ENERGY', '$/MWH', 1),
        ('ENERGY', '$/mwh', 2),
        ('ENERGY', '$/MW-MO', 3),
        ('ENERGY', 'FLAT RATE', 4),
        ('ENERGY', NULL, 5),
        ('CAPACITY', '$/MW-MO', 6),
        ('CAPACITY', '$/KW', 7),
        ('CAPACITY', 'CENTS/KWH', 8),
        ('BOOKED OUT POWER', '$/MWH', 9),
        ('UPLIFT', 'MW/MIN', 10),
        ('UPLIFT', '$/MWH', 11)
    ) AS t(product, unit, row_id)
),
cases AS (
    -- 1 of 4 ENERGY rows with a unit is outside the allowed energy and neutral units
    -- ($/MW-MO). NULL units are skipped and matching ignores case: rate 0.25.
    SELECT 'energy_fail' AS case_name, TRUE AS expect_failure, COUNT(*) AS n_failures
    FROM ({{ unit_mismatch_check(
        'unit_table', 'unit', energy_unit_patterns() + neutral_unit_patterns(), 0.2,
        row_condition="product = 'ENERGY'"
    ) }})
    UNION ALL
    SELECT 'energy_pass', FALSE, COUNT(*)
    FROM ({{ unit_mismatch_check(
        'unit_table', 'unit', energy_unit_patterns() + neutral_unit_patterns(), 0.4,
        row_condition="product = 'ENERGY'"
    ) }})
    UNION ALL
    -- 1 of 3 CAPACITY rows has energy units.
    SELECT 'capacity_fail', TRUE, COUNT(*)
    FROM ({{ unit_mismatch_check(
        'unit_table', 'unit', power_unit_patterns() + neutral_unit_patterns(), 0.2,
        row_condition="product = 'CAPACITY'"
    ) }})
    UNION ALL
    -- Pooling ENERGY and BOOKED OUT POWER dilutes the rate to 1 of 5 (0.2).
    SELECT 'pooled_pass', FALSE, COUNT(*)
    FROM ({{ unit_mismatch_check(
        'unit_table', 'unit', energy_unit_patterns() + neutral_unit_patterns(), 0.3,
        row_condition="product IN ('ENERGY', 'BOOKED OUT POWER')"
    ) }})
    UNION ALL
    -- A unit that is neither energy, power nor neutral fails, not just the wrong
    -- dimension: 1 of 2 UPLIFT rows (MW/MIN) is outside the energy units.
    SELECT 'unknown_unit_fail', TRUE, COUNT(*)
    FROM ({{ unit_mismatch_check(
        'unit_table', 'unit', energy_unit_patterns() + neutral_unit_patterns(), 0.4,
        row_condition="product = 'UPLIFT'"
    ) }})
    UNION ALL
    -- A caller-supplied list replaces the defaults, so FLAT RATE is no longer allowed:
    -- 2 of 4 ENERGY rows ($/MW-MO and FLAT RATE) are outside it.
    SELECT 'custom_allowed_fail', TRUE, COUNT(*)
    FROM ({{ unit_mismatch_check(
        'unit_table', 'unit', energy_unit_patterns(), 0.4,
        row_condition="product = 'ENERGY'"
    ) }})
    UNION ALL
    -- Extending the allowed list can make the same rows pass.
    SELECT 'custom_allowed_pass', FALSE, COUNT(*)
    FROM ({{ unit_mismatch_check(
        'unit_table', 'unit', energy_unit_patterns() + ['%-MO', 'FLAT%'], 0.0,
        row_condition="product = 'ENERGY'"
    ) }})
    UNION ALL
    -- No matching rows at all means nothing to fail.
    SELECT 'no_rows_pass', FALSE, COUNT(*)
    FROM ({{ unit_mismatch_check(
        'unit_table', 'unit', energy_unit_patterns(), 0.0,
        row_condition="product = 'NOTHING'"
    ) }})
)

SELECT * FROM cases
WHERE (expect_failure AND n_failures = 0)
    OR (NOT expect_failure AND n_failures > 0)
