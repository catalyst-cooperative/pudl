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

import duckdb
import pandas as pd
from dagster import asset

from pudl import logging_helpers
from pudl.workspace.datastore import Datastore
from pudl.workspace.setup import PudlPaths

logger = logging_helpers.get_logger(__name__)

VCERARE_PAGES = {
    "offshore_wind_power_140m": "Wind_Power_140m_Offshore_county.csv",
    "onshore_wind_power_100m": "Wind_Power_100m_Onshore_county.csv",
    "fixed_solar_pv_lat_upv": "Fixed_SolarPV_Lat_UPV_county.csv",
}


def extract_year(
    vcerare_page: str,
    conn: duckdb.DuckDBPyConnection,
    year: int,
    ds: Datastore,
):
    """Extract one year of one vcerare table from CSV's and write to parquet."""
    table_name = f"raw_vcerare__{vcerare_page}"
    table_parquet_path = PudlPaths().output_dir / "parquet" / table_name
    table_parquet_path.mkdir(exist_ok=True)
    with (
        ds.get_zipfile_resource("vcerare", year=year) as zf,
        zf.open(f"{year}/{VCERARE_PAGES[vcerare_page]}") as csv,
    ):
        rel = conn.read_csv(csv)
        columns = rel.columns
        col_map = {col: col.lower() for col in columns}
        col_map[columns[0]] = "hour_of_year"
        (
            rel.select(
                f"{year} AS report_year, "
                + ", ".join(
                    [f'"{col}" AS "{clean_col}"' for col, clean_col in col_map.items()]
                )
            ).to_parquet(str(table_parquet_path / f"{year}.parquet"))
        )


def raw_vcerare_asset_factory(vcerare_page: str):
    """Construct an asset to extract a single page from vcerare."""
    table_name = f"raw_vcerare__{vcerare_page}"

    @asset(
        name=table_name,
        required_resource_keys={
            "datastore",
            "dataset_settings",
            "vcerare_duckdb_transformer",
        },
    )
    def _extract_asset(context):
        """Extract a single year/page combo."""
        ds = context.resources.datastore
        vcerare_duckdb = context.resources.vcerare_duckdb_transformer
        vcerare_settings = context.resources.dataset_settings.vcerare

        with vcerare_duckdb.get_connection() as conn:
            for year in vcerare_settings.years:
                extract_year(vcerare_page, conn, year, ds)
            (
                conn.read_parquet(
                    str(PudlPaths().output_dir / "parquet" / table_name),
                    union_by_name=True,
                ).to_view(table_name, replace=True)
            )

    return _extract_asset


raw_vcerare_assets = [raw_vcerare_asset_factory(page) for page in VCERARE_PAGES]


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
