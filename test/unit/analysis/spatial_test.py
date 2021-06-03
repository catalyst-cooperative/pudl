"""Tests for timeseries anomalies detection and imputation."""

import geopandas as gpd
import pandas as pd
from geopandas.testing import assert_geodataframe_equal
from shapely.geometry import MultiPolygon, Polygon

from pudl.analysis.spatial import dissolve, explode, self_union

gpd.options.display_precision = 0


def test_dissolve():
    """Test mergining of geometries and non-spatial attributes."""
    gdf = gpd.GeoDataFrame({
        'geometry': gpd.GeoSeries([
            Polygon([(0, 0), (0, 1), (3, 1), (3, 0)]),
            Polygon([(3, 0), (3, 1), (4, 1), (4, 0)])
        ]),
        'id': [0, 0],
        'ids': [0, 1],
        'x': [3.0, 1.0]
    })

    output_gdf = dissolve(gdf, by='id', func={'ids': tuple, 'x': 'sum'}, how='union')

    expected_gdf = gpd.GeoDataFrame({
        'geometry': gpd.GeoSeries([
            Polygon([(0, 0), (0, 1), (3, 1), (4, 1), (4, 0), (3, 0), (0, 0)]),
        ]),
        'ids': [(0, 1), ],
        'x': [4.0, ],
    }, index=pd.Index(data=[0, ], name="id"))

    assert_geodataframe_equal(output_gdf, expected_gdf)


def test_explode():
    """Test exploting geometries and non-spatial attributes."""
    gdf = gpd.GeoDataFrame({
        'geometry': gpd.GeoSeries([
            MultiPolygon([
                Polygon([(0, 0), (0, 1), (1, 1), (1, 0)]),
                Polygon([(1, 1), (2, 1), (2, 2), (1, 2)])
            ]),
            Polygon([(1, 1), (1, 2), (2, 2), (2, 1)]),
        ]),
        'x': [0, 1],
        'y': [4.0, 8.0],
    })

    output_gdf = explode(gdf, ratios=['y'])
    expected_gdf = gpd.GeoDataFrame({
        'geometry': gpd.GeoSeries([
            Polygon([(0, 0), (0, 1), (1, 1), (1, 0), (0, 0)]),
            Polygon([(1, 1), (2, 1), (2, 2), (1, 2), (1, 1)]),
            Polygon([(1, 1), (1, 2), (2, 2), (2, 1), (1, 1)]),
        ], index=[0, 0, 1]),
        'x': [0, 0, 1],
        'y': [2.0, 2.0, 8.0],
    })
    assert_geodataframe_equal(output_gdf, expected_gdf)

    # Explode, dissolve, and see if we get the same thing back again
    exploded = explode(gdf, ratios=['y']).reset_index()
    dissolved = (
        dissolve(exploded, by='index', func={'x': 'first', 'y': 'sum'})
        .rename_axis(None)
    )
    assert_geodataframe_equal(dissolved, gdf)


def test_self_union():
    """Test self union of geometries and non-spatial attributes."""
    gdf = gpd.GeoDataFrame({
        'geometry': gpd.GeoSeries([
            Polygon([(0, 0), (0, 2), (2, 2), (2, 0)]),
            Polygon([(1, 1), (3, 1), (3, 3), (1, 3)]),
            Polygon([(1, 1), (1, 2), (2, 2), (2, 1)]),
        ]),
        'x': [0, 1, 2],
        'y': [4.0, 8.0, 1.0],
    })
    result_one = self_union(gdf)

    expected_one = gpd.GeoDataFrame({
        'geometry': gpd.GeoSeries([
            Polygon([(0, 0), (0, 2), (1, 2), (1, 1), (2, 1), (2, 0), (0, 0)]),
            Polygon([(1, 2), (2, 2), (2, 1), (1, 1), (1, 2)]),
            Polygon([(1, 2), (2, 2), (2, 1), (1, 1), (1, 2)]),
            Polygon([(1, 2), (2, 2), (2, 1), (1, 1), (1, 2)]),
            Polygon([(2, 2), (1, 2), (1, 3), (3, 3), (3, 1), (2, 1), (2, 2)]),
        ], index=[(0, ), (0, 1, 2), (0, 1, 2), (0, 1, 2), (1, )]),
        'x': [0, 0, 1, 2, 1],
        'y': [4.0, 4.0, 8.0, 1.0, 8.0],
    })
    assert_geodataframe_equal(result_one, expected_one)

    result_two = self_union(gdf, ratios=['y'])
    expected_two = gpd.GeoDataFrame({
        'geometry': gpd.GeoSeries([
            Polygon([(0, 0), (0, 2), (1, 2), (1, 1), (2, 1), (2, 0), (0, 0)]),
            Polygon([(1, 2), (2, 2), (2, 1), (1, 1), (1, 2)]),
            Polygon([(1, 2), (2, 2), (2, 1), (1, 1), (1, 2)]),
            Polygon([(1, 2), (2, 2), (2, 1), (1, 1), (1, 2)]),
            Polygon([(2, 2), (1, 2), (1, 3), (3, 3), (3, 1), (2, 1), (2, 2)]),
        ], index=[(0, ), (0, 1, 2), (0, 1, 2), (0, 1, 2), (1, )]),
        'x': [0, 0, 1, 2, 1],
        'y': [3.0, 1.0, 2.0, 1.0, 6.0],
    })
    assert_geodataframe_equal(result_two, expected_two)
