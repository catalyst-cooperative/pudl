"""Module to perform data cleaning functions on EIA930 data tables."""

import pandas as pd
from dagster import (
    asset,
)

import pudl

logger = pudl.logging_helpers.get_logger(__name__)


@asset
def _core_eia930__hourly_balancing_authority_net_generation(
    raw_eia930__balance: pd.DataFrame,
) -> pd.DataFrame:
    """Transforms raw_eia923__balance dataframe.

    Extract the net generation information from the balance table and extract fuel type
    from the net generation columns.
    """
    ng_df = raw_eia930__balance
    qual_cols = [
        "report_datetime_local",
        "report_datetime_utc",
        "balancing_authority_code_eia",
        # report_date
        # eia_region_code,
        # report_hour_local
    ]
    total_ng_cols = [
        "net_generation_imputed_mw",
        "net_generation_adjusted_mw",
        "net_generation_mw",
    ]
    ng_cols = list(ng_df.filter(like="net_generation"))
    # Remove total net gen values from the list of NG cols
    ng_cols = [x for x in ng_cols if x not in total_ng_cols]
    ng_df = ng_df[qual_cols + ng_cols]
    ng_df = ng_df.melt(id_vars=qual_cols, value_vars=ng_cols)
    # Add "raw" to melted columns that aren't adjusted or imputed
    not_imputed_or_adjusted = ~ng_df["variable"].str.endswith(("adjusted", "imputed"))
    ng_df.loc[not_imputed_or_adjusted, "variable"] = ng_df.variable.str.replace(
        "_mw", "_raw_mw"
    )
    # Extract fuel and calculation type from the original column names
    capture_pattern = r"net_generation_(.*)_(adjusted|imputed|raw)"
    ng_df[["fuel_source", "calc_type"]] = ng_df.variable.str.extract(capture_pattern)
    ng_df = (
        ng_df.reset_index()
    )  # This step is necessary to create unique index values on which to pivot
    # Turn calc_types into columns
    ng_df = (
        ng_df.pivot(
            columns="calc_type",
            values="value",
            index=qual_cols + ["fuel_source", "index"],
        )
        .reset_index()
        .drop(columns="index")
    )

    return ng_df


# core_eia930__hourly_subregion_demand
# core_eia930__hourly_balancing_authority_demand
# core_eia930__hourly_balancing_authority_interchange
# core_eia930__assn_balancing_authority_subregion
