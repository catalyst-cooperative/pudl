"""Functions for compiling derived aspects of the EIA 930 data."""

import pandas as pd
from dagster import asset

from pudl.analysis.timeseries_cleaning import (
    ImputeTimeseriesSettings,
    impute_timeseries_asset_factory,
)

BA_TIMEZONE_MAP = {
    "CISO": "America/Los_Angeles",
    "ERCO": "America/Chicago",
    "ISNE": "America/New_York",
    "MISO": "America/Chicago",
    "NYIS": "America/New_York",
    "PJM": "America/New_York",
    "PNM": "America/Denver",
    "SWPP": "America/Chicago",
}


@asset(
    compute_kind="Python",
    required_resource_keys={"dataset_settings"},
)
def _out_eia930__hourly_subregion_demand(
    core_eia930__hourly_subregion_demand: pd.DataFrame,
) -> pd.DataFrame:
    """Adds timezone column and combined ID with BA/subregion used for imputation."""
    core_eia930__hourly_subregion_demand["timezone"] = (
        core_eia930__hourly_subregion_demand["balancing_authority_code_eia"].map(
            BA_TIMEZONE_MAP
        )
    )
    core_eia930__hourly_subregion_demand["combined_subregion_ba_id"] = (
        core_eia930__hourly_subregion_demand["balancing_authority_code_eia"]
        + core_eia930__hourly_subregion_demand["balancing_authority_subregion_code_eia"]
    )
    return core_eia930__hourly_subregion_demand


imputed_subregion_demand_assets = impute_timeseries_asset_factory(  # pragma: no cover
    input_asset_name="_out_eia930__hourly_subregion_demand",
    output_asset_name="out_eia930__hourly_subregion_demand",
    years_from_context=lambda context: [
        int(half_year[:4])
        for half_year in context.resources.dataset_settings.eia.eia930.half_years
        if "2025" not in half_year
    ],
    value_col="demand_reported_mwh",
    imputed_value_col="demand_imputed_pudl_mwh",
    id_col="combined_subregion_ba_id",
    settings=ImputeTimeseriesSettings(method_overrides={2019: "tnn"}),
)
