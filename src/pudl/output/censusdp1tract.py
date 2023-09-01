"""Functions for reading data out of the Census DP1 SQLite Database."""

import geopandas as gpd
import pandas as pd
import sqlalchemy as sa
from dagster import AssetIn, AssetsDefinition, asset

import pudl

logger = pudl.logging_helpers.get_logger(__name__)


def census_asset_factory(layer: str) -> AssetsDefinition:
    """An asset factory for finished EIA tables."""

    @asset(
        ins={"censusdp1tract_to_sqlite": AssetIn("censusdp1tract_to_sqlite")},
        name=f"core_censusdp1__entity_{layer}",
    )
    def census_layer(censusdp1tract_to_sqlite, **kwargs) -> gpd.GeoDataFrame:
        """Select one layer from the Census DP1 database.

        Uses information within the Census DP1 database to set the coordinate reference
        system and to identify the column containing the geometry. The geometry column
        is renamed to "geom" as that's the default withing Geopandas. No other column
        names or types are altered.
        """
        census_conn = f"sqlite:///{censusdp1tract_to_sqlite}"
        dp1_engine = sa.create_engine(census_conn)

        def get_layer(layer, dp1_engine):
            if not isinstance(layer, str):
                raise TypeError(
                    f"Argument 'layer' must be a string, got arg of type {layer}."
                )
            layer = layer.lower()
            if layer not in ["state", "county", "tract"]:
                raise ValueError(
                    "Census DP1 layer must be one of 'state', 'county' or 'tract', "
                    f"but got {layer}."
                )
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
                params=(table_name,),
            )
            if len(df) != 1:
                raise AssertionError(
                    f"Expected exactly 1 geometry description, but found {len(df)}"
                )

            geom_col = df.at[0, "geom_col"]
            crs_auth_str = f"{df.at[0, 'auth_name']}:{df.at[0, 'auth_srid']}".lower()

            gdf = gpd.read_postgis(
                table_name, dp1_engine, geom_col=geom_col, crs=crs_auth_str
            )
            gdf.rename_geometry("geometry", inplace=True)

            return gdf

        return get_layer(layer, dp1_engine)

    return census_layer


census_dp1_layers = [
    census_asset_factory(layer) for layer in ["state", "county", "tract"]
]
