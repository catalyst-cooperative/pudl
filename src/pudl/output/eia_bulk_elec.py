"""Output tables derived from the EIA Bulk Electricity data."""
import pandas as pd
from dagster import asset

import pudl

logger = pudl.logging_helpers.get_logger(__name__)


@asset(io_manager_key=None, compute_kind="Python")
def state_average_fuel_costs_eia(
    fuel_receipts_costs_aggs_eia: pd.DataFrame,
) -> pd.DataFrame:
    """Get state-level average fuel costs from EIA's bulk electricity data.

    This data is used to fill in missing fuel prices in the
    :ref:`fuel_receipts_costs_eia923` table. It was created as a drop-in replacement
    for data we were previously obtaining from EIA's unreliable API.
    """
    aggregates = fuel_receipts_costs_aggs_eia.loc[
        :,
        [
            "report_date",
            "sector_agg",
            "temporal_agg",
            "fuel_agg",
            "geo_agg",
            "fuel_cost_per_mmbtu",
        ],
    ]
    aggregates = aggregates[
        (aggregates.sector_agg == "all_electric_power")
        & (aggregates.temporal_agg == "monthly")
        & (aggregates.fuel_agg.isin(["all_coal", "petroleum_liquids", "natural_gas"]))
        & (aggregates.fuel_cost_per_mmbtu.notnull())
    ].drop(columns=["sector_agg", "temporal_agg"])
    fuel_map = {  # convert to fuel_type_code_pudl categories
        "all_coal": "coal",
        "natural_gas": "gas",
        "petroleum_liquids": "oil",
    }
    aggregates["fuel_type_code_pudl"] = aggregates["fuel_agg"].map(fuel_map)
    aggregates.drop(columns="fuel_agg", inplace=True)

    col_rename_dict = {
        "geo_agg": "state",
        "fuel_cost_per_mmbtu": "bulk_agg_fuel_cost_per_mmbtu",
    }
    aggregates.rename(columns=col_rename_dict, inplace=True)
    return aggregates
