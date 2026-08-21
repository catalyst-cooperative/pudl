"""Integration tests verifying GeoParquet outputs are spec-compliant.

These tests check that geo assets written by PudlParquetIOManager produce valid
GeoParquet 1.0.0 files readable by geopandas, pandas, polars, and DuckDB >= 1.5.

The DuckDB test is the primary regression guard: before the CRS metadata format
was switched from WKT to PROJJSON, DuckDB 1.5 raised "Geoparquet column
'geometry' has invalid CRS" when loading the spatial extension. The geometry
column should now be recognised as a native GEOMETRY type.

Run with --live-pudl-output to skip the ETL pre-build and use existing outputs:

    pixi run pytest --no-cov --live-pudl-output tests/integration/dagster/geoparquet_test.py
"""

from pathlib import Path

import duckdb
import geopandas as gpd  # noqa: ICN002
import pandas as pd
import polars as pl
import pytest

from pudl.workspace.setup import PudlPaths

_GEO_TABLE_NAMES = [
    "out_censusdp1tract__counties",
    "out_ferc714__georeferenced_respondents",
]


@pytest.fixture(params=_GEO_TABLE_NAMES)
def geo_parquet_path(request, prebuilt_outputs, pudl_test_paths: PudlPaths) -> Path:
    """Resolve path to a GeoParquet output file, skipping if absent.

    Depends on ``prebuilt_outputs`` so the ETL runs before the path is checked.
    """
    table_name: str = request.param
    path = pudl_test_paths.parquet_path(table_name)
    return path


def test_geoparquet_readable_by_geopandas(geo_parquet_path: Path) -> None:
    """GeoParquet outputs have valid CRS and non-empty geometry when read by geopandas."""
    gdf = gpd.read_parquet(geo_parquet_path)

    assert not gdf.empty
    assert "geometry" in gdf.columns
    assert not gdf.geometry.isna().all(), "geometry column is entirely null"
    assert gdf.crs is not None, "no CRS attached to GeoDataFrame"
    assert gdf.crs.to_epsg() is not None, (
        f"CRS is not representable as an EPSG code: {gdf.crs}"
    )


def test_geoparquet_readable_by_pandas(geo_parquet_path: Path) -> None:
    """GeoParquet outputs can be opened by plain pandas without errors.

    Geometry is returned as a binary (WKB) column, which is expected behavior
    when reading GeoParquet without the geopandas geometry-aware reader.
    """
    df = pd.read_parquet(geo_parquet_path)

    assert not df.empty
    assert "geometry" in df.columns


def test_geoparquet_readable_by_polars(geo_parquet_path: Path) -> None:
    """GeoParquet outputs can be scanned by polars without errors."""
    df = pl.scan_parquet(geo_parquet_path).collect()

    assert not df.is_empty()
    assert "geometry" in df.columns


def test_geoparquet_duckdb_geometry_type(geo_parquet_path: Path) -> None:
    """DuckDB >= 1.5 reads the geometry column as GEOMETRY type, not BLOB.

    When DuckDB's spatial extension is loaded, it inspects the ``geo`` key in the
    Parquet file-level metadata. If the CRS is stored in PROJJSON format (a dict),
    the column is exposed as ``GEOMETRY('EPSG:xxxx')``. If it was stored in the old
    WKT string format, DuckDB 1.5 raises "Geoparquet column 'geometry' has invalid
    CRS" instead.
    """
    con = duckdb.connect()
    try:
        con.load_extension("spatial")
    except duckdb.Error:
        con.install_extension("spatial")
        con.load_extension("spatial")

    try:
        result = con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{geo_parquet_path}')"  # noqa: S608
        ).fetchall()
    finally:
        con.close()

    geometry_col = next((row for row in result if row[0] == "geometry"), None)
    assert geometry_col is not None, (
        f"No 'geometry' column found in DESCRIBE output for {geo_parquet_path.name}"
    )
    assert geometry_col[1].startswith("GEOMETRY"), (
        f"Expected GEOMETRY type but got {geometry_col[1]!r}. "
        "This likely means the CRS metadata is stored as WKT rather than PROJJSON, "
        "which DuckDB >= 1.5 rejects."
    )
