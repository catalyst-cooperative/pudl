"""Functions for reading data out of the Census DP1 SQLite Database."""

from pathlib import Path
from typing import Literal

import geopandas as gpd
import pandas as pd
import sqlalchemy as sa

import pudl
from pudl.convert.censusdp1tract_to_sqlite import censusdp1tract_to_sqlite
from pudl.workspace.datastore import Datastore

logger = pudl.logging_helpers.get_logger(__name__)


def get_layer(
    layer: Literal["state", "county", "tract"], pudl_settings=None, ds=None
) -> gpd.GeoDataFrame:
    """Select one layer from the Census DP1 database.

    Uses information within the Census DP1 database to set the coordinate
    reference system and to identify the column containing the geometry. The
    geometry column is renamed to "geom" as that's the default withing
    Geopandas. No other column names or types are altered.

    Args:
        layer (str): Which set of geometries to read, must be one of "state",
            "county", or "tract".
        pudl_settings (dict or None): A dictionary of PUDL settings, including
            paths to various resources like the Census DP1 SQLite database. If
            None, the user defaults are used.

    Returns:
        geopandas.GeoDataFrame
    """
    if not isinstance(layer, str):
        raise TypeError(f"Argument 'layer' must be a string, got arg of type {layer}.")
    layer = layer.lower()
    if layer not in ["state", "county", "tract"]:
        raise ValueError(
            "Census DP1 layer must be one of 'state', 'county' or 'tract', "
            f"but got {layer}."
        )
    if pudl_settings is None:
        pudl_settings = pudl.workspace.setup.get_defaults()
    if ds is None:
        ds = Datastore()
    # Check if we have the Census DP1 database. If not, create it.
    if not Path(pudl_settings["pudl_out"], "censusdp1tract.sqlite").exists():
        logger.info("Census DP1 SQLite DB is missing. Creating it.")
        censusdp1tract_to_sqlite(pudl_settings=pudl_settings, ds=ds)

    dp1_engine = sa.create_engine(pudl_settings["censusdp1tract_db"])

    table_name = f"{layer}_2010census_dp1"
    df = pd.read_sql(
        """
SELECT geom_cols.f_table_name as table_name,
       geom_cols.f_geometry_column as geom_col,
       crs.auth_name as auth_name,
       crs.auth_srid as auth_srid
  FROM geometry_columns geom_cols
 INNER JOIN spatial_ref_sys crs
    ON geom_cols.srid = crs.srid
 WHERE table_name = ?
""",
        dp1_engine,
        params=[table_name],
    )
    if len(df) != 1:
        raise AssertionError(
            f"Expected exactly 1 geometry description, but found {len(df)}"
        )

    geom_col = df.at[0, "geom_col"]
    crs_auth_str = f"{df.at[0, 'auth_name']}:{df.at[0, 'auth_srid']}".lower()

    gdf = gpd.read_postgis(table_name, dp1_engine, geom_col=geom_col, crs=crs_auth_str)
    gdf.rename_geometry("geometry", inplace=True)

    return gdf
