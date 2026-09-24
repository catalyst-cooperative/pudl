"""Tests for writing service territory geometries to GeoParquet."""

from pathlib import Path

import geopandas as gpd  # noqa: ICN002
import pyarrow.parquet as pq
from shapely.geometry import box

import pudl
from pudl.analysis.service_territory import _save_geoparquet


def test_save_geoparquet_compression(tmp_path: Path, mocker) -> None:
    """Service territory files are compressed with the codec and geometry level set centrally in ``pudl``."""
    gdf = gpd.GeoDataFrame(
        {
            "report_date": ["2020-01-01"] * 2,
            "utility_id_eia": [2, 1],
            "geometry": gpd.GeoSeries([box(0, 0, 1, 1), box(1, 1, 2, 2)], crs=4269),
        }
    )
    mocker.patch("pudl.PARQUET_GEOMETRY_COMPRESSION_LEVEL", 11)
    spy = mocker.spy(gpd.GeoDataFrame, "to_parquet")

    _save_geoparquet(
        gdf,
        entity_type="utility",
        dissolve=False,
        limit_by_state=False,
        output_dir=tmp_path,
    )

    assert spy.call_args.kwargs["compression"] == pudl.PARQUET_COMPRESSION
    assert spy.call_args.kwargs["compression_level"] == 11
    path = tmp_path / "utility_geometry.parquet"
    assert (
        pq.read_metadata(path).row_group(0).column(0).compression
        == pudl.PARQUET_COMPRESSION.upper()
    )
