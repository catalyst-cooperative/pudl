"""Extract VCE Resource Adequacy Renewable Energy (RARE) Power Dataset.

This dataset has 1,000s of columns, so we don't want to manually specify a rename on
import because we'll pivot these to a column in the transform step. We adapt the
standard extraction infrastructure to simply read in the data.

Each annual zip folder contains a folder with three files:
Wind_Power_140m_Offshore_county.csv
Wind_Power_100m_Onshore_county.csv
Fixed_SolarPV_Lat_UPV_county.csv

The drive also contains one more CSV file: vce_county_lat_long_fips_table.csv. This gets
read in when the fips partition is set to True.
"""

from io import BytesIO
from pathlib import Path

import duckdb
import pandas as pd
from dagster import AssetSpec, asset, multi_asset

from pudl import logging_helpers

logger = logging_helpers.get_logger(__name__)

VCERARE_PAGES = {
    "raw_vcerare__offshore_wind_power_140m": "Wind_Power_140m_Offshore_county.csv",
    "raw_vcerare__onshore_wind_power_100m": "Wind_Power_100m_Onshore_county.csv",
    "raw_vcerare__fixed_solar_pv_lat_upv": "Fixed_SolarPV_Lat_UPV_county.csv",
}


def _clean_columns(
    table_relation: duckdb.DuckDBPyRelation,
) -> duckdb.DuckDBPyRelation:
    """Apply basic cleaning to column names."""
    columns = table_relation.columns
    col_map = {col: col.lower().replace(".", "").replace("-", "_") for col in columns}

    # The first column is never named, but is always the ``hour_of_year`` column
    col_map[columns[0]] = "hour_of_year"
    col_map["year"] = "report_year"

    # Rename all columns
    return table_relation.select(
        ", ".join([f'"{col}" AS "{clean_col}"' for col, clean_col in col_map.items()])
    )


@multi_asset(
    specs=[AssetSpec(table_name) for table_name in VCERARE_PAGES],
    required_resource_keys={
        "pudl_parquet_transformer",
        "dataset_settings",
    },
)
def extract_vcerare(context):
    """Extract data from all vcerare pages and write to parquet files."""
    dataset_settings = context.resources.dataset_settings
    pudl_parquet_transformer = context.resources.pudl_parquet_transformer
    pudl_parquet_transformer.extract_csv_to_parquet(
        "vcerare",
        VCERARE_PAGES,
        partition_paths=[
            ({"year": year}, Path(f"{year}/"))
            for year in dataset_settings.vcerare.years
        ],
        add_partition_columns=["year"],
        custom_transforms=_clean_columns,
    )


@asset(required_resource_keys={"datastore", "dataset_settings"})
def raw_vcerare__lat_lon_fips(context) -> pd.DataFrame:
    """Extract lat/lon to FIPS and county mapping CSV.

    This dataframe is static, so it has a distinct partition from the other datasets and
    its extraction is controlled by a boolean in the ETL run.
    """
    ds = context.resources.datastore
    partition_settings = context.resources.dataset_settings.vcerare
    if partition_settings.fips:
        return pd.read_csv(
            BytesIO(ds.get_unique_resource("vcerare", fips=partition_settings.fips))
        )
    return pd.DataFrame()
