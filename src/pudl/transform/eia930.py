"""Module to perform data cleaning functions on EIA930 data tables."""

import pandas as pd
from dagster import asset

import pudl

logger = pudl.logging_helpers.get_logger(__name__)


@asset
def core_eia930__hourly_balancing_authority_net_generation(
    raw_eia930__balance: pd.DataFrame,
) -> pd.DataFrame:
    """Transforms raw_eia923__balance dataframe.

    Extract the net generation information from the balance table and extract fuel type
    from the net generation columns.
    """
    qual_cols = [
        "report_datetime_local",
        "report_datetime_utc",
        "balancing_authority_code_eia",
        # report_date
        # eia_region_code,
        # report_hour_local
    ]
    # We don't want to retain totals, which are redundant
    total_netgen_cols = [
        "net_generation_imputed_mw",
        "net_generation_adjusted_mw",
        "net_generation_mw",
    ]
    netgen_cols = list(raw_eia930__balance.filter(like="net_generation"))
    # Remove total net gen values from the list of net gen cols
    netgen_cols = [x for x in netgen_cols if x not in total_netgen_cols]
    netgen = raw_eia930__balance[qual_cols + netgen_cols]

    def _simplify_columns(col: str) -> str:
        return (
            col.removeprefix("net_generation_")
            .removesuffix("_mw")
            .replace("all_petroleum_products", "oil")
            .replace("hydropower_and_pumped_storage", "hydro")
            .replace("natural_gas", "gas")
            .replace("other_fuel_sources", "other")
        )

    # Rename columns so that they contain only the energy source and the level of
    # processing, so it's easy to construct a multi-index to unstack below
    # Doing these manipulations while the values are in the column names rather than
    # after they've been turned into a categorical column with millions of entries
    # is much faster.
    netgen_renamed = (
        netgen.rename(_simplify_columns, axis="columns")
        .rename(
            columns={
                "coal": "coal_raw",
                "gas": "gas_raw",
                "hydro": "hydro_raw",
                "oil": "oil_raw",
                "other": "other_raw",
                "nuclear": "nuclear_raw",
                "solar": "solar_raw",
                "unknown_fuel_sources": "unknown_raw",
                "wind": "wind_raw",
            },
        )
        .set_index(
            [
                "report_datetime_local",
                "report_datetime_utc",
                "balancing_authority_code_eia",
            ]
        )
    )

    # Prepare a multi-index for the columns so that we can stack cleanly
    netgen_renamed.columns = pd.MultiIndex.from_tuples(
        [x.split("_") for x in netgen_renamed.columns], names=["energy_source", None]
    )
    return (
        netgen_renamed.stack(level=0, future_stack=True)
        .rename(columns=lambda x: x + "_net_generation_mw")
        .reset_index()
        .astype({"energy_source": "string"})
        .astype(
            {
                "balancing_authority_code_eia": pd.CategoricalDtype(),
                "energy_source": pd.CategoricalDtype(),
            }
        )
    )


# core_eia930__hourly_subregion_demand
# core_eia930__hourly_balancing_authority_demand
# core_eia930__hourly_balancing_authority_interchange
# core_eia930__assn_balancing_authority_subregion
