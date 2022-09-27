"""Module to help update the id's associated with utilities."""

import importlib
import logging
from pathlib import Path

import pandas as pd
import sqlalchemy as sa

import pudl.helpers

logger = logging.getLogger(__name__)

PUDL_SETTINGS: dict[str, str] = pudl.workspace.setup.get_defaults()

PUDL_ID_MAP_XLSX = importlib.resources.open_binary(
    "pudl.package_data.glue", "pudl_id_mapping.xlsx"
)
UTIL_ID_PUDL_MAP_CSV = importlib.resources.open_text(
    "pudl.package_data.glue", "utility_id_pudl.csv"
)

UTIL_ID_FERC_MAP_CSV = importlib.resources.open_text(
    "pudl.package_data.glue", "utility_id_ferc1.csv"
)

##################################
# Get the stored IDS as dataframes
##################################


def get_util_ids_ferc1_xbrl():
    """Grab the utlity ids (reported as `entity_id`) in the FERC1 XBRL database."""
    ferc1_xbrl_engine = sa.create_engine(PUDL_SETTINGS["ferc1_xbrl_db"])
    entity_ids = (
        pd.read_sql(
            "SELECT entity_id, RespondentLegalName FROM identification_001_duration",
            ferc1_xbrl_engine,
        )
        .drop_duplicates(subset=["entity_id"])
        .rename(
            columns={
                "entity_id": "utility_id_ferc1_xbrl",
                "RespondentLegalName": "utility_name_ferc1",
            }
        )
    )
    return entity_ids


def get_util_ids_ferc1_csv() -> pd.DataFrame:
    """Grab the FERC1 utility ID map."""
    return pd.read_csv(UTIL_ID_FERC_MAP_CSV.name).convert_dtypes()


def get_util_ids_pudl():
    """Grab the PUDL utility ID map."""
    return pd.read_csv(UTIL_ID_PUDL_MAP_CSV.name).convert_dtypes()


#########
# Helpers
#########


def update_id_col(
    old_ids: pd.DataFrame, new_ids: pd.DataFrame, id_col: str
) -> pd.DataFrame:
    """Append new ID's found in one table onto another.

    Args:
        old_ids: a table with the existing ID's
        new_ids: a dataframe with the new ID's
        id_col: the ID column
    """
    ids_updated = pd.merge(
        new_ids,
        old_ids.drop_duplicates(subset=[id_col]),
        on=[id_col],
        validate="m:1",
        indicator=True,
        how="outer",
    )
    new_ids = len(ids_updated[ids_updated._merge == "right_only"])
    logger.info(f"Found {new_ids} new {id_col}")
    # only include the columns that existed in the original table
    return ids_updated[list(old_ids.columns)]


def autoincrement_from_max(df: pd.DataFrame, id_col: str) -> pd.DataFrame:
    """Fill in missing IDs via auto-incrementing from the max available ID.

    Args:
        df: the table which has ``id_col`` as a column and you want to fill in missing
            values with an auto-incremented ID starting at the max value of the existing
            values in ``id_col``.
        id_col: the column name. Column must contain integer values.
    """
    max_id = df[id_col].max()
    null_mask = df[id_col].isnull()
    null_len = len(df.loc[null_mask])
    df.loc[null_mask, id_col] = pd.RangeIndex(max_id, max_id + null_len) + 1
    return df


##########################################
# Update the stored IDs with new additions
##########################################


def update_util_id_ferc1_map_xbrl():
    """Update the FERC1 utility ID map with any missing XBRL IDs."""
    util_ids_xbrl = get_util_ids_ferc1_xbrl()
    util_ids_ferc1 = get_util_ids_ferc1_csv()

    util_ids_ferc1_new = update_id_col(
        new_ids=util_ids_xbrl, old_ids=util_ids_ferc1, id_col="utility_id_ferc1_xbrl"
    ).pipe(autoincrement_from_max, id_col="utility_id_ferc1")
    util_ids_ferc1_new.to_csv(Path(UTIL_ID_FERC_MAP_CSV.name), index=False)


def update_util_id_pudl_ferc1():
    """Update the PUD: utility ID map with any missing FERC1 IDs."""
    util_ids_ferc1 = get_util_ids_ferc1_csv()
    util_ids_pudl = get_util_ids_pudl()

    util_ids_pudl_new = update_id_col(
        new_ids=util_ids_ferc1, old_ids=util_ids_pudl, id_col="utility_id_ferc1"
    ).pipe(autoincrement_from_max, id_col="utility_id_pudl")
    # update the ferc1 names based on the most recent xbrl name
    util_ids_ferc_w_names = util_ids_ferc1.merge(
        get_util_ids_ferc1_xbrl(),
        on=["utility_id_ferc1_xbrl"],
        how="inner",
        validate="m:1",
    )[["utility_id_ferc1", "utility_name_ferc1"]]

    util_ids_pudl = (
        util_ids_pudl.merge(
            util_ids_ferc_w_names,
            on=["utility_id_ferc1"],
            how="outer",
            suffixes=("", "_new"),
        )
        .assign(
            utility_name_ferc1=lambda x: x.utility_name_ferc1_new.fillna(
                x.utility_name_ferc1
            )
        )
        .drop(columns=["utility_name_ferc1_new"])
    )

    util_ids_pudl_new.to_csv(Path(UTIL_ID_PUDL_MAP_CSV.name), index=False)


#############
# Temporary !
#############


def _tmp_update_utility_id_pudl_with_new_ferc1_ids(
    util_id_ferc1: pd.DataFrame,
    util_id_pudl: pd.DataFrame,
    export_to_pudl_map: bool,
) -> pd.DataFrame:
    """Generate a mapping of old ID's to new ID's and export into the pudl mapping.

    This function generates a new tab in the pudl mappping spreadsheet that can be used
    to replace the utility_id_ferc's in the plant mapping sheet with a little
    index-match.

    NOTE: This is a temporary function meant to help the transition to these new IDs
    and should probably be deleted afterward.
    """
    # extract the unique mappings of old ids and rename for merge
    util_id_ferc1_unique = (
        util_id_ferc1[["utility_id_ferc1", "utility_id_ferc1_dbf"]].drop_duplicates(
            subset=["utility_id_ferc1_dbf"]
        )
    ).rename(
        columns={
            "utility_id_ferc1": "utilty_id_ferc1_new",
            "utility_id_ferc1_dbf": "utility_id_ferc1",
        }
    )

    util_id_pudl = util_id_pudl.merge(
        util_id_ferc1_unique, on=["utility_id_ferc1"], how="left", validate="m:1"
    )
    non_mapped = util_id_pudl[
        util_id_pudl.utility_id_ferc1.notnull()
        & util_id_pudl.utilty_id_ferc1_new.isnull()
    ]
    if not non_mapped.empty:
        raise AssertionError(
            f"FERC1 ID's found that have not been updated: {non_mapped}"
        )

    util_ids_changed = util_id_pudl[
        util_id_pudl.utilty_id_ferc1_new != util_id_pudl.utility_id_ferc1
    ][["utilty_id_ferc1_new", "utility_id_ferc1"]].drop_duplicates()
    logger.info(f"Updating {len(util_ids_changed)} utility_id_ferc1's.")
    util_id_pudl.loc[:, "utility_id_ferc1"] = util_id_pudl.loc[:, "utilty_id_ferc1_new"]
    util_id_pudl = util_id_pudl.drop(columns=["utilty_id_ferc1_new"])
    if export_to_pudl_map:
        pudl.helpers.save_to_workbook(
            file_path=PUDL_ID_MAP_XLSX,
            sheets_df_dict={"utilities_ferc1": util_ids_changed},
        )
    return util_ids_changed
