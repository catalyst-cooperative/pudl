"""Functions for reading data out of the Census DP1 SQLite Database."""

from typing import Literal, TypedDict

import geopandas as gpd
import pandas as pd
import sqlalchemy as sa
from dagster import AssetIn, AssetsDefinition, asset

import pudl

logger = pudl.logging_helpers.get_logger(__name__)


class LayerParams(TypedDict):
    """Simple class defining the expected structure of the layer processing params."""

    plural: str
    rename: dict[str, str]


def census_asset_factory(
    layer: Literal["state", "county", "tract"],
) -> AssetsDefinition:
    """An asset factory for finished EIA tables."""
    common_rename: dict[str, str] = {
        "aland10": "land_area",
        "awater10": "water_area",
        "intptlat10": "internal_point_latitude",
        "intptlon10": "internal_point_longitude",
        "shape_length": "shape_length",
        "shape_area": "shape_area",
    }

    layer_params: dict[str, LayerParams] = {
        "state": {
            "plural": "states",
            "rename": common_rename
            | {
                "geoid10": "state_id_fips",
                "name10": "state_name",
                "stusps10": "state",
            },
        },
        "county": {
            "plural": "counties",
            "rename": common_rename
            | {
                "geoid10": "county_id_fips",
                "namelsad10": "county",
                "funcstat10": "functional_status_code_census",
            },
        },
        "tract": {
            "plural": "tracts",
            "rename": common_rename
            | {
                "geoid10": "tract_id_fips",
                "namelsad10": "tract_name",
            },
        },
    }

    @asset(
        ins={
            "raw_censusdp1tract__all_tables": AssetIn("raw_censusdp1tract__all_tables")
        },
        name=f"out_censusdp1tract__{layer_params[layer]['plural']}",
        io_manager_key="geoparquet_io_manager",
    )
    def census_layer(raw_censusdp1tract__all_tables, **kwargs) -> gpd.GeoDataFrame:
        """Select one layer from the Census DP1 database.

        Uses information within the Census DP1 database to set the coordinate reference
        system and to identify the column containing the geometry. The geometry column
        is renamed to "geometry" as that's the default within GeoPandas. Non-demographic
        columns are renamed for readability, but the ~175 demographic data column names
        are left untouched.
        """
        census_conn = f"sqlite:///{raw_censusdp1tract__all_tables}"
        dp1_engine = sa.create_engine(census_conn)

        def get_layer(
            layer: Literal["state", "county", "tract"], dp1_engine: sa.Engine
        ):
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

            geom_col = df.loc[0, "geom_col"]
            crs_auth_str = f"{df.loc[0, 'auth_name']}:{df.loc[0, 'auth_srid']}".lower()

            gdf: gpd.GeoDataFrame = (
                gpd.read_postgis(
                    sql=table_name, con=dp1_engine, geom_col=geom_col, crs=crs_auth_str
                )
                .rename_geometry("geometry")
                .drop(columns=["objectid"])
                .rename(columns=layer_params[layer]["rename"])
                .astype(
                    {
                        "land_area": float,
                        "water_area": float,
                        "internal_point_latitude": float,
                        "internal_point_longitude": float,
                    }
                )
            )

            return gdf

        return get_layer(layer, dp1_engine)

    return census_layer


census_dp1_layers = [
    census_asset_factory(layer) for layer in ["state", "county", "tract"]
]
