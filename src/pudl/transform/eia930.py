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
    # Select only the columns relevant to the BA net generation table
    netgen_cols = list(raw_eia930__balance.filter(like="net_generation"))
    interchange_cols = list(raw_eia930__balance.filter(like="interchange"))
    netgen = raw_eia930__balance[qual_cols + netgen_cols + interchange_cols]

    # Rename columns so that they contain only the energy source and the level of
    # processing, so it's easy to construct a multi-index to unstack below
    # Doing these manipulations while the values are in the column names rather than
    # after they've been turned into a categorical column with millions of entries
    # is much faster.
    netgen_renamed = netgen.rename(
        lambda col: col.removeprefix("net_generation_").removesuffix("_mw"),
        axis="columns",
    ).set_index(qual_cols)

    # Prepare a multi-index for the columns so that we can stack cleanly
    netgen_renamed.columns = pd.MultiIndex.from_tuples(
        [x.split("_") for x in netgen_renamed.columns], names=["energy_source", None]
    )
    netgen_stacked = (
        netgen_renamed.stack(level=0, future_stack=True)
        .rename(columns=lambda x: f"net_generation_{x}_mw")
        .reset_index()
        .astype({"energy_source": "string"})
        .astype(
            {
                "balancing_authority_code_eia": pd.CategoricalDtype(),
                "energy_source": pd.CategoricalDtype(),
            }
        )
    )
    # TODO[zaneselvans] 2024-04-20: Verify that sum of net generation from all fuels
    # adds up to the total And then drop the total rows. Note that currently there are
    # some big differences between the calculated total and the reported total.

    # return netgen_stacked[netgen_stacked["energy_source"] != "total"

    return netgen_stacked


# core_eia930__hourly_subregion_demand
# core_eia930__hourly_balancing_authority_demand
# core_eia930__hourly_balancing_authority_interchange
# core_eia930__assn_balancing_authority_subregion
