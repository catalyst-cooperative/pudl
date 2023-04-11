"""Denormalized versions of the EIA 860 tables."""
import pandas as pd
from dagster import asset

import pudl


@asset(io_manager_key="pudl_sqlite_io_manager", compute_kind="Python")
def denorm_ownership_eia860(
    denorm_plants_utilities_eia: pd.DataFrame,
    ownership_eia860: pd.DataFrame,
) -> pd.DataFrame:
    """A denormalized version of the EIA 860 ownership table.

    TODO: Convert to SQL view?

    Args:
        denorm_plants_utilities_eia: Denormalized table containing plant and utility
            names and IDs.
        ownership_eia860: EIA 860 ownership table.

    Returns:
        A denormalized version of the EIA 860 ownership table.
    """
    pu_df = denorm_plants_utilities_eia.loc[
        :,
        [
            "plant_id_eia",
            "plant_id_pudl",
            "plant_name_eia",
            "utility_name_eia",
            "utility_id_pudl",
            "report_date",
        ],
    ]
    own_df = pd.merge(
        ownership_eia860, pu_df, on=["report_date", "plant_id_eia"], how="left"
    ).dropna(
        subset=["report_date", "plant_id_eia", "generator_id", "owner_utility_id_eia"]
    )
    first_cols = [
        "report_date",
        "plant_id_eia",
        "plant_id_pudl",
        "plant_name_eia",
        "utility_id_eia",
        "utility_id_pudl",
        "utility_name_eia",
        "generator_id",
        "owner_utility_id_eia",
        "owner_name",
    ]

    # Re-arrange the columns for easier readability:
    return pudl.helpers.organize_cols(own_df, first_cols)
