"""A collection of denormalized EIA assets."""
import pandas as pd
from dagster import asset

import pudl
from pudl.metadata.fields import apply_pudl_dtypes
from pudl.output.eia860 import fill_in_missing_ba_codes


@asset(io_manager_key="pudl_sqlite_io_manager", compute_kind="Python")
def denorm_utilities_eia(
    utilities_entity_eia: pd.DataFrame,
    utilities_eia860: pd.DataFrame,
    utilities_eia: pd.DataFrame,
):
    """Pull all fields from the EIA Utilities table.

    Args:
        utilities_entity_eia: EIA utility entity table.
        utilities_eia860: EIA 860 annual utility table.
        utilities_eia: Associations between EIA utilities and pudl utility IDs.

    Returns:
        A DataFrame containing all the fields of the EIA 860 Utilities table.
    """
    utilities_eia = utilities_eia[["utility_id_eia", "utility_id_pudl"]]
    out_df = pd.merge(
        utilities_entity_eia, utilities_eia860, how="left", on=["utility_id_eia"]
    )
    out_df = pd.merge(out_df, utilities_eia, how="left", on=["utility_id_eia"])
    out_df = (
        out_df.assign(report_date=lambda x: pd.to_datetime(x.report_date))
        .dropna(subset=["report_date", "utility_id_eia"])
        .pipe(apply_pudl_dtypes, group="eia")
    )
    first_cols = [
        "report_date",
        "utility_id_eia",
        "utility_id_pudl",
        "utility_name_eia",
    ]
    out_df = pudl.helpers.organize_cols(out_df, first_cols)
    return out_df


@asset(io_manager_key="pudl_sqlite_io_manager", compute_kind="Python")
def denorm_plants_eia(
    plants_entity_eia: pd.DataFrame,
    plants_eia860: pd.DataFrame,
    plants_eia: pd.DataFrame,
    utilities_eia: pd.DataFrame,
):
    """Pull all fields from the EIA Plants tables.

    Args:
        plants_entity_eia: EIA plant entity table.
        plants_eia860: EIA 860 annual plant attribute table.
        plants_eia: Associations between EIA plants and pudl utility IDs.

    Returns:
        pandas.DataFrame: A DataFrame containing all the fields of the EIA 860
        Plants table.
    """
    plants_eia860 = plants_eia860.assign(
        report_date=lambda x: pd.to_datetime(x.report_date)
    )

    plants_eia = plants_eia[["plant_id_eia", "plant_id_pudl"]]

    out_df = (
        pd.merge(plants_entity_eia, plants_eia860, how="left", on=["plant_id_eia"])
        .merge(plants_eia, how="left", on=["plant_id_eia"])
        .merge(utilities_eia, how="left", on=["utility_id_eia"])
        .dropna(subset=["report_date", "plant_id_eia"])
        .pipe(fill_in_missing_ba_codes)
        .pipe(apply_pudl_dtypes, group="eia")
    )
    return out_df
