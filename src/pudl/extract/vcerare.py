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

from collections import defaultdict
from collections.abc import Callable
from io import BytesIO
from pathlib import Path

import pandas as pd
from dagster import AssetOut, asset, multi_asset

from pudl import logging_helpers
from pudl.helpers import (
    ParquetData,
    duckdb_extract_zipped_csv,
    persist_table_as_parquet,
)

logger = logging_helpers.get_logger(__name__)

VCERARE_PAGES = {
    "Wind_Power_140m_Offshore_county.csv": "raw_vcerare__offshore_wind_power_140m",
    "Wind_Power_100m_Onshore_county.csv": "raw_vcerare__onshore_wind_power_100m",
    "Fixed_SolarPV_Lat_UPV_county.csv": "raw_vcerare__fixed_solar_pv_lat_upv",
}

# VCE RARE switched the raw hour_of_year column from an integer hour index
# (1-8760) to an ISO timestamp starting with this report year. Shared with
# pudl.transform.vcerare so the two don't drift independently.
DATETIME_HOUR_OF_YEAR_START_YEAR = 2024


def _clean_column_name(col: str) -> str:
    """Match the raw VCE RARE column naming convention to PUDL's snake_case."""
    return col.lower().replace(".", "").replace("-", "_")


def _vcerare_column_types(year: int) -> Callable[[list[str]], dict[str, str]]:
    """Build a DuckDB ``columns`` schema from a raw VCE RARE CSV header row.

    Every column is a per-county/subregion capacity factor (``DOUBLE``) except the
    first, unnamed column, which is always ``hour_of_year`` and whose type depends
    on the report year (see ``DATETIME_HOUR_OF_YEAR_START_YEAR``). The set of
    county/subregion columns is not stable across vintages, so it's always derived
    from the actual header row rather than hardcoded.
    """
    hour_of_year_type = (
        "TIMESTAMP" if year >= DATETIME_HOUR_OF_YEAR_START_YEAR else "BIGINT"
    )

    def _column_types(header_row: list[str]) -> dict[str, str]:
        return {"hour_of_year": hour_of_year_type} | {
            _clean_column_name(col): "DOUBLE" for col in header_row[1:]
        }

    return _column_types


@multi_asset(
    outs={table_name: AssetOut() for table_name in VCERARE_PAGES.values()},
    required_resource_keys={
        "datastore",
        "global_data_config",
    },
)
def extract_vcerare(
    context,
) -> tuple[dict[int, ParquetData], dict[int, ParquetData], dict[int, ParquetData]]:
    """Extract data from all vcerare pages and write to parquet files."""
    extracted_tables = defaultdict(dict)

    # Loop through all years in settings and extract
    for year in context.resources.global_data_config.pudl.vcerare.years:
        partitions = {"year": year}
        # Extract each raw table, clean column names, then offload to parquet
        for page, relation in duckdb_extract_zipped_csv(
            dataset="vcerare",
            partitions=partitions,
            pages=VCERARE_PAGES.keys(),
            datasore=context.resources.datastore,
            zip_path=Path(f"{year}/"),
            column_types=_vcerare_column_types(year),
        ):
            # Collect ParquetData objects for each year/page combo
            extracted_tables[VCERARE_PAGES[page]].update(
                {
                    year: persist_table_as_parquet(
                        table_data=relation.select(f"*, {year} as report_year"),
                        table_name=VCERARE_PAGES[page],
                        partitions=partitions,
                        use_native_duckdb_writer=True,
                    )
                }
            )
    # For each raw table, return a dict mapping years to a ParquetData object
    return tuple(extracted_tables.values())


@asset(required_resource_keys={"datastore", "global_data_config"})
def raw_vcerare__lat_lon_fips(context) -> pd.DataFrame:
    """Extract lat/lon to FIPS and county mapping CSV.

    This dataframe is static, so it has a distinct partition from the other datasets and
    its extraction is controlled by a boolean in the ETL run.
    """
    ds = context.resources.datastore
    partition_data_config = context.resources.global_data_config.pudl.vcerare
    if partition_data_config.fips:
        return pd.read_csv(
            BytesIO(ds.get_unique_resource("vcerare", fips=partition_data_config.fips))
        )
    return pd.DataFrame()
