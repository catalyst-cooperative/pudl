-- Two adjacent 1 degree squares, A (lon -100 to -99) and B (lon -99 to -98), both from
-- lat 40 to 41. At this latitude 0.01 degrees of longitude is about 0.85 km, so with a
-- 5 km tolerance, 0.02 degrees is well inside the tolerance and 0.5 degrees is well
-- outside it.
WITH test_regions AS (
    SELECT
        region_id,
        region_name,
        ST_SetCRS(ST_GeomFromText(wkt), 'EPSG:4269') AS geometry
    FROM (VALUES
        ('A', 'Region A', 'POLYGON((-100 40, -99 40, -99 41, -100 41, -100 40))'),
        ('B', 'Region B', 'POLYGON((-99 40, -98 40, -98 41, -99 41, -99 40))'),
    ) AS t(region_id, region_name, wkt)
),

test_points AS (
    SELECT * FROM (VALUES
        -- Inside the reported region.
        (1, 40.5, -99.5, 'A'),
        -- Inside B, far from A: fails, and is located in B.
        (2, 40.5, -98.5, 'A'),
        -- Inside B but about 0.85 km from A: within tolerance.
        (3, 40.5, -98.99, 'A'),
        -- In no region, far from A: fails, with no located region.
        (4, 40.5, -97.0, 'A'),
        -- In no region but about 1.7 km from A: within tolerance.
        (5, 40.5, -100.02, 'A'),
        -- Reported region matches no region, point inside A: fails, no distance.
        (6, 40.5, -99.5, 'Z'),
        -- Reported region matches no region, point in no region: also fails.
        (7, 40.5, -97.0, 'Z'),
        -- Missing latitude, longitude, or region: can't be inconsistent.
        (8, NULL, -97.0, 'A'),
        (9, 40.5, NULL, 'A'),
        (10, 40.5, -97.0, NULL),
    ) AS t(point_id, latitude, longitude, region_id)
),

expected_failures AS (
    SELECT * FROM (VALUES
        (2, 'point_in_other_region', 'B', FALSE),
        (4, 'point_in_no_region', NULL, FALSE),
        (6, 'reported_region_not_found', 'A', TRUE),
        (7, 'reported_region_not_found', NULL, TRUE),
    ) AS t(point_id, failure_reason, located_region_id, distance_is_null)
),

observed_failures AS (
    SELECT
        point_id,
        failure_reason,
        located_region_id,
        distance_to_reported_meters IS NULL AS distance_is_null
    FROM ({{
        facility_outside_reported_region(
            api.Relation.create(identifier='test_points'),
            region_id_column='region_id',
            regions=api.Relation.create(identifier='test_regions'),
            label_column='region_name',
            tolerance_meters=5000,
            crs='EPSG:4269',
        )
    }})
),

-- Any row in only one of the two sets is a mismatch.
mismatches AS (
    (SELECT 'unexpected' AS kind, * FROM (
        SELECT * FROM observed_failures
        EXCEPT
        SELECT * FROM expected_failures
    ))
    UNION ALL
    (SELECT 'missing' AS kind, * FROM (
        SELECT * FROM expected_failures
        EXCEPT
        SELECT * FROM observed_failures
    ))
)

SELECT * FROM mismatches
