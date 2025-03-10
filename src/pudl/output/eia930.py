"""Functions for compiling derived aspects of the EIA 930 data."""

import pandas as pd
from dagster import asset

import pudl.analysis.timeseries_cleaning

BA_TIMEZONE_MAP = {
    "CISO": "America/Los_Angeles",
    "ERCO": "America/Denver",
    "ISNE": "America/New_York",
    "MISO": "America/Chicago",
    "NYIS": "America/New_York",
    "PJM": "America/New_York",
    "PNM": "America/Denver",
    "SWPP": "America/Chicago",
}


@asset(
    io_manager_key="parquet_io_manager",
    compute_kind="Python",
    required_resource_keys={"dataset_settings"},
)
def out_eia930__hourly_subregion_demand(
    context,
    core_eia930__hourly_subregion_demand: pd.DataFrame,
) -> pd.DataFrame:
    """Impute hourly sub-region demand."""
    core_eia930__hourly_subregion_demand["timezone"] = (
        core_eia930__hourly_subregion_demand["balancing_authority_code_eia"].map(
            BA_TIMEZONE_MAP
        )
    )
    return pudl.analysis.timeseries_cleaning.impute_timeseries(
        core_eia930__hourly_subregion_demand,
        years=[
            int(half_year[:4])
            for half_year in context.resources.dataset_settings.eia.eia930.half_years
        ],
        value_col="demand_reported_mwh",
        id_col="balancing_authority_subregion_code_eia",
    )
