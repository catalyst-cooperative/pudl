"""A collection of denormalized EIA assets."""
import pandas as pd
from dagster import asset

import pudl
from pudl.metadata.fields import apply_pudl_dtypes


@asset(io_manager_key="pudl_sqlite_io_manager", compute_kind="Python")
def denorm_utilities_eia860(
    utilities_entity_eia: pd.DataFrame,
    utilities_eia860: pd.DataFrame,
    utilities_eia: pd.DataFrame,
):
    """Pull all fields from the EIA860 Utilities table.

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
