-- Shared logic for expect_facility_in_reported_state and
-- expect_facility_in_reported_county. Returns one row for each row of model whose
-- (latitude, longitude) point lies outside the region the row reports, e.g. a plant
-- whose coordinates are in Ohio but whose state is Georgia.
--
-- The model must have latitude and longitude columns. The regions relation must have a
-- geometry column with a CRS (as in GeoParquet). The points are assumed to be in the
-- same CRS as the regions, which is read from the regions' geometry type.
--
-- A row passes if it has a null latitude, longitude, or region ID (a missing value can't
-- be inconsistent), or if its point is within tolerance_meters of its reported region
-- (coordinates and boundaries are imprecise). A point within no region at all (e.g.
-- offshore, or in another country) is not exempt: it fails if it is too far from its
-- reported region. A reported region ID that matches no region always fails.
-- Failures include distance_to_reported_meters, the distance from the point to its
-- reported region (NULL if there is no such region), and located_<region_id_column> and
-- located_<label_column>, for the region that actually contains the point (NULL if the
-- point is within no region). failure_reason says why the row failed:
--   reported_region_not_found: the reported ID matches no region (checked first)
--   point_in_no_region: the point is within no region, and too far from its own
--   point_in_other_region: the point is within another region, and too far from its
--   own
--
-- Done in two steps for speed: first find rows not within their own reported region (one
-- point-in-polygon test per row), then look up the containing region for just those
-- rows. Testing every row against every region is more than 20x slower.
--
-- Distances are measured in EPSG:5070 (NAD83 / Conus Albers), which is accurate enough
-- for the US at tolerances of a few km, but distorts somewhat in AK, HI, and PR.
-- Requires the DuckDB spatial extension (see profiles.yml).
--
-- crs is optional. If it is not given, the CRS is read from the regions relation, which
-- must then be a real table or view rather than a CTE (e.g. in a unit test).
{% macro facility_outside_reported_region(
    model, region_id_column, regions, label_column, tolerance_meters, crs=none
) %}

{# ST_SetCRS needs a literal, so read the CRS from the regions when rendering. #}
{% set regions_crs = crs %}
{% if regions_crs is none and execute %}
    {% set regions_crs = run_query(
        'SELECT ST_CRS(geometry) FROM ' ~ regions ~ ' LIMIT 1'
    ).columns[0].values()[0] %}
{% endif %}

WITH points AS (
    SELECT
        *,
        ST_SetCRS(ST_Point(longitude, latitude), '{{ regions_crs }}') AS _point
    FROM {{ model }}
    WHERE
        latitude IS NOT NULL
        AND longitude IS NOT NULL
        AND {{ region_id_column }} IS NOT NULL
),

-- Rows whose point is not within the region they report.
outside_reported_region AS (
    SELECT
        points.*,
        reported.geometry AS _reported_geometry
    FROM points
    LEFT JOIN {{ regions }} AS reported USING ({{ region_id_column }})
    WHERE
        reported.geometry IS NULL
        OR NOT ST_Intersects(reported.geometry, points._point)
),

-- Projecting is expensive, so only do it for rows outside their reported region. The
-- distance is NULL if the reported region ID matches no region.
distances AS (
    SELECT
        *,
        ROUND(
            ST_Distance(
                ST_Transform(_reported_geometry, 'EPSG:5070', always_xy := true),
                ST_Transform(_point, 'EPSG:5070', always_xy := true)
            )
        ) AS distance_to_reported_meters
    FROM outside_reported_region
),

beyond_tolerance AS (
    SELECT *
    FROM distances
    WHERE
        distance_to_reported_meters IS NULL
        OR distance_to_reported_meters > {{ tolerance_meters }}
)

SELECT
    CASE
        WHEN beyond_tolerance.distance_to_reported_meters IS NULL
            THEN 'reported_region_not_found'
        WHEN located.geometry IS NULL THEN 'point_in_no_region'
        ELSE 'point_in_other_region'
    END AS failure_reason,
    located.{{ region_id_column }} AS located_{{ region_id_column }},
    located.{{ label_column }} AS located_{{ label_column }},
    beyond_tolerance.distance_to_reported_meters,
    beyond_tolerance.* EXCLUDE (_point, _reported_geometry, distance_to_reported_meters)
FROM beyond_tolerance
LEFT JOIN {{ regions }} AS located
    ON ST_Intersects(located.geometry, beyond_tolerance._point)

{% endmacro %}
