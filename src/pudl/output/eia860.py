"""Denormalized versions of the EIA 860 tables."""

import pandas as pd
from dagster import asset

import pudl
from pudl.metadata.codes import CODE_METADATA


@asset(io_manager_key="pudl_io_manager", compute_kind="Python")
def out_eia860__yearly_ownership(
    _out_eia__plants_utilities: pd.DataFrame,
    core_eia860__scd_ownership: pd.DataFrame,
    core_pudl__assn_eia_pudl_utilities: pd.DataFrame,
) -> pd.DataFrame:
    """A denormalized version of the EIA 860 ownership table.

    Args:
        _out_eia__plants_utilities: Denormalized table containing plant and utility
            names and IDs.
        core_eia860__scd_ownership: EIA 860 ownership table.
        core_pudl__assn_eia_pudl_utilities: Table of associations between EIA utility IDs and
            PUDL Utility IDs.

    Returns:
        A denormalized version of the EIA 860 ownership table.
    """
    pu_df = _out_eia__plants_utilities.loc[
        :,
        [
            "plant_id_eia",
            "plant_id_pudl",
            "plant_name_eia",
            "report_date",
        ],
    ]
    own_df = pd.merge(
        core_eia860__scd_ownership,
        pu_df,
        on=["report_date", "plant_id_eia"],
        how="left",
    ).dropna(
        subset=["report_date", "plant_id_eia", "generator_id", "owner_utility_id_eia"]
    )
    util_df = core_pudl__assn_eia_pudl_utilities.loc[
        :, ["utility_id_eia", "utility_id_pudl"]
    ]
    own_df = own_df.merge(
        util_df, how="left", left_on="owner_utility_id_eia", right_on="utility_id_eia"
    )
    first_cols = [
        "report_date",
        "plant_id_eia",
        "plant_id_pudl",
        "plant_name_eia",
        "owner_utility_id_eia",
        "utility_id_pudl",
        "owner_utility_name_eia",
        "generator_id",
    ]

    # Re-arrange the columns for easier readability:
    return pudl.helpers.organize_cols(own_df, first_cols)


@asset(io_manager_key="pudl_io_manager", compute_kind="Python")
def out_eia860__yearly_emissions_control_equipment(
    core_eia860__scd_emissions_control_equipment: pd.DataFrame,
    _out_eia__plants_utilities: pd.DataFrame,
) -> pd.DataFrame:
    """A denormalized version of the EIA 860 emission control equipment table.

    Args:
        core_eia860__scd_emissions_control_equipment: EIA 860 emissions control equipment table.
        _out_eia__plants_utilities: Denormalized table containing plant and utility
            names and IDs.

    Returns:
        A denormalized version of the EIA 860 emissions control equipment table.
    """
    pu_df = _out_eia__plants_utilities.loc[
        :,
        [
            "plant_id_eia",
            "plant_id_pudl",
            "plant_name_eia",
            "utility_id_eia",
            "utility_id_pudl",
            "utility_name_eia",
            "report_date",
        ],
    ]
    pu_df = pu_df.assign(report_year=lambda x: x.report_date.dt.year).drop(
        columns=["report_date"]
    )
    emce_df = pd.merge(
        core_eia860__scd_emissions_control_equipment,
        pu_df,
        on=["report_year", "plant_id_eia"],
        how="left",
    )

    # Add a column for operational status
    emce_df["operational_status"] = emce_df.operational_status_code.str.upper().map(
        pudl.helpers.label_map(
            CODE_METADATA["core_eia__codes_operational_status"]["df"],
            from_col="code",
            to_col="operational_status",
            null_value=pd.NA,
        )
    )

    return emce_df
