"""Tests for timeseries anomalies detection and imputation."""

import logging
import re

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
from geopandas import GeoDataFrame, GeoSeries
from geopandas.testing import assert_geodataframe_equal
from shapely.geometry import GeometryCollection, MultiPolygon, Polygon

from pudl.analysis.spatial import (
    check_gdf,
    dissolve,
    explode,
    overlay,
    polygonize,
    self_union,
)

gpd.options.display_precision = 0

POLY = Polygon([(0, 0), (0, 1), (1, 1), (1, 0)])
ZERO_POLY = Polygon([(0, 0), (0, 0), (0, 0), (0, 0)])

logger = logging.getLogger(__name__)


@pytest.mark.parametrize(
    "gdf,exc,pattern",
    [
        (None, TypeError, r"Object is not a GeoDataFrame"),
        (GeoDataFrame({"x": [0]}), AttributeError, r"GeoDataFrame has no geometry"),
        (
            GeoDataFrame({"geometry": [0]}),
            AttributeError,
            r"GeoDataFrame has no geometry",
        ),
        (
            GeoDataFrame(geometry=[GeometryCollection()]),
            ValueError,
            r"Geometry contains non-(Multi)Polygon geometries",
        ),
        (
            GeoDataFrame(geometry=[ZERO_POLY]),
            ValueError,
            r"Geometry contains (Multi)Polygon geometries with zero area",
        ),
        (
            GeoDataFrame(geometry=[MultiPolygon([POLY, ZERO_POLY])]),
            ValueError,
            r"MultiPolygon contains Polygon geometries with zero area",
        ),
    ],
)
def test_check_gdf(gdf, exc, pattern):
    """Test GeoDataFrame validation function."""
    with pytest.raises(exc) as err:
        check_gdf(gdf)
    assert err.match(re.escape(pattern))

    assert check_gdf(GeoDataFrame(geometry=[POLY])) is None


def test_polygonize():
    """Test conversion of Geometries into (Multi)Polygons."""
    # A collection with one non-zero-area Polygon is returned as a Polygon.
    geom1 = GeometryCollection([POLY, ZERO_POLY])
    result1 = polygonize(geom1)
    assert result1.geom_type == "Polygon"
    assert result1.area == 1.0

    # A collection with multiple non-zero-area polygons is returned as a MultiPolygon.
    geom2 = GeometryCollection([POLY, POLY])
    result2 = polygonize(geom2)
    assert result2.geom_type == "MultiPolygon"
    assert result2.area == 2.0

    # Zero-area geometries are not permitted.
    with pytest.raises(ValueError) as err:
        _ = polygonize(ZERO_POLY)
    assert err.match("Geometry has zero area")


def test_explode():
    """Test exploting geometries and non-spatial attributes."""
    gdf = GeoDataFrame(
        {
            "geometry": GeoSeries(
                [
                    MultiPolygon(
                        [
                            Polygon([(0, 0), (0, 1), (1, 1), (1, 0)]),
                            Polygon([(1, 1), (2, 1), (2, 2), (1, 2)]),
                        ]
                    ),
                    Polygon([(1, 1), (1, 2), (2, 2), (2, 1)]),
                ]
            ),
            "x": [0, 1],
            "y": [4.0, 8.0],
        }
    )

    output_gdf = explode(gdf, ratios=["y"])
    expected_gdf = GeoDataFrame(
        {
            "geometry": GeoSeries(
                [
                    Polygon([(0, 0), (0, 1), (1, 1), (1, 0)]),
                    Polygon([(1, 1), (2, 1), (2, 2), (1, 2)]),
                    Polygon([(1, 1), (1, 2), (2, 2), (2, 1)]),
                ],
                index=[0, 0, 1],
            ),
            "x": [0, 0, 1],
            "y": [2.0, 2.0, 8.0],
        }
    )
    assert_geodataframe_equal(output_gdf, expected_gdf)

    # Explode, dissolve, and see if we get the same thing back again
    exploded = explode(gdf, ratios=["y"]).reset_index()
    dissolved = dissolve(
        exploded, by="index", func={"x": "first", "y": "sum"}
    ).rename_axis(None)
    assert_geodataframe_equal(dissolved, gdf)


def test_self_union():
    """Test self union of geometries and non-spatial attributes."""
    gdf = GeoDataFrame(
        {
            "geometry": GeoSeries(
                [
                    Polygon([(0, 0), (0, 2), (2, 2), (2, 0)]),
                    Polygon([(1, 1), (3, 1), (3, 3), (1, 3)]),
                    Polygon([(1, 1), (1, 2), (2, 2), (2, 1)]),
                ]
            ),
            "x": [0, 1, 2],
            "y": [4.0, 8.0, 1.0],
        }
    )

    result_one = self_union(gdf)
    expected_one = GeoDataFrame(
        {
            "geometry": GeoSeries(
                [
                    Polygon([(0, 0), (0, 2), (1, 2), (1, 1), (2, 1), (2, 0)]),
                    Polygon([(1, 2), (2, 2), (2, 1), (1, 1)]),
                    Polygon([(1, 2), (2, 2), (2, 1), (1, 1)]),
                    Polygon([(1, 2), (2, 2), (2, 1), (1, 1)]),
                    Polygon([(2, 2), (1, 2), (1, 3), (3, 3), (3, 1), (2, 1)]),
                ],
                index=[(0,), (0, 1, 2), (0, 1, 2), (0, 1, 2), (1,)],
            ),
            "x": [0, 0, 1, 2, 1],
            "y": [4.0, 4.0, 8.0, 1.0, 8.0],
        }
    )
    assert_geodataframe_equal(result_one, expected_one)

    result_two = self_union(gdf, ratios=["y"])
    expected_two = GeoDataFrame(
        {
            "geometry": GeoSeries(
                [
                    Polygon([(0, 0), (0, 2), (1, 2), (1, 1), (2, 1), (2, 0)]),
                    Polygon([(1, 2), (2, 2), (2, 1), (1, 1)]),
                    Polygon([(1, 2), (2, 2), (2, 1), (1, 1)]),
                    Polygon([(1, 2), (2, 2), (2, 1), (1, 1)]),
                    Polygon([(2, 2), (1, 2), (1, 3), (3, 3), (3, 1), (2, 1)]),
                ],
                index=[(0,), (0, 1, 2), (0, 1, 2), (0, 1, 2), (1,)],
            ),
            "x": [0, 0, 1, 2, 1],
            "y": [3.0, 1.0, 2.0, 1.0, 6.0],
        }
    )
    assert_geodataframe_equal(result_two, expected_two)


def test_dissolve():
    """Test mergining of geometries and non-spatial attributes."""
    gdf = GeoDataFrame(
        {
            "geometry": GeoSeries(
                [
                    Polygon([(0, 0), (0, 1), (3, 1), (3, 0)]),
                    Polygon([(3, 0), (3, 1), (4, 1), (4, 0)]),
                ]
            ),
            "id": [0, 0],
            "ids": [0, 1],
            "x": [3.0, 1.0],
        }
    )

    output_gdf = dissolve(gdf, by="id", func={"ids": tuple, "x": "sum"}, how="union")

    expected_gdf = GeoDataFrame(
        {
            "geometry": GeoSeries(
                [
                    Polygon([(0, 0), (0, 1), (3, 1), (4, 1), (4, 0), (3, 0), (0, 0)]),
                ]
            ),
            "ids": [
                (0, 1),
            ],
            "x": [
                4.0,
            ],
        },
        index=pd.Index(
            data=[
                0,
            ],
            name="id",
        ),
    )

    assert_geodataframe_equal(output_gdf, expected_gdf)


def test_overlay():
    """Test overlaying of spatial layers and non-spatial attributes."""
    gdf_a = GeoDataFrame(
        {"geometry": GeoSeries([Polygon([(0, 0), (0, 1), (3, 1), (3, 0)])]), "a": [3.0]}
    )
    gdf_b = GeoDataFrame(
        {"geometry": GeoSeries([Polygon([(2, 0), (2, 1), (4, 1), (4, 0)])]), "b": [8.0]}
    )

    expected_intersection = GeoDataFrame(
        {
            "a": [3.0],
            "b": [8.0],
            "geometry": GeoSeries([Polygon([(2, 1), (3, 1), (3, 0), (2, 0)])]),
        }
    )
    result_intersection = overlay(gdf_a, gdf_b, how="intersection")
    assert_geodataframe_equal(expected_intersection, result_intersection)

    expected_union = GeoDataFrame(
        {
            "a": [3.0, 3.0, np.nan],
            "b": [8.0, np.nan, 8.0],
            "geometry": GeoSeries(
                [
                    Polygon([(2, 1), (3, 1), (3, 0), (2, 0)]),
                    Polygon([(0, 0), (0, 1), (2, 1), (2, 0)]),
                    Polygon([(3, 1), (4, 1), (4, 0), (3, 0)]),
                ]
            ),
        }
    )
    result_union = overlay(gdf_a, gdf_b, how="union")
    assert_geodataframe_equal(expected_union, result_union)

    expected_ratios = GeoDataFrame(
        {
            "geometry": GeoSeries(
                [
                    Polygon([(2, 1), (3, 1), (3, 0), (2, 0)]),
                    Polygon([(0, 0), (0, 1), (2, 1), (2, 0)]),
                    Polygon([(3, 1), (4, 1), (4, 0), (3, 0)]),
                ]
            ),
            "a": [1.0, 2.0, np.nan],
            "b": [4.0, np.nan, 4.0],
        }
    )
    result_ratios = overlay(gdf_a, gdf_b, how="union", ratios=["a", "b"])
    assert_geodataframe_equal(expected_ratios, result_ratios)


def test_get_data_columns():
    """Test extraction of non-spatial column names from GeoDataFrame."""
    logger.info("No unit tests exist for pudl.analysis.spatial.get_data_columns()")
