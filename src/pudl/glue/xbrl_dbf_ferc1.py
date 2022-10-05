"""Module to help update the id's associated with utilities."""

import logging
from pathlib import Path

import pandas as pd

from pudl.glue.ferc1_eia import (
    UTIL_ID_FERC_MAP_CSV,
    UTIL_ID_PUDL_MAP_CSV,
    get_util_ids_ferc1_raw_xbrl,
)

logger = logging.getLogger(__name__)


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


def update_util_id_ferc1_map_xbrl(utilities_ferc1_xbrl, utilities_ferc1):
    """Update the FERC1 utility ID map with any missing XBRL IDs."""
    util_ids_ferc1_new = update_id_col(
        new_ids=utilities_ferc1_xbrl,
        old_ids=utilities_ferc1,
        id_col="utility_id_ferc1_xbrl",
    ).pipe(autoincrement_from_max, id_col="utility_id_ferc1")
    util_ids_ferc1_new.to_csv(Path(UTIL_ID_FERC_MAP_CSV.name), index=False)


def update_util_id_pudl_ferc1(utilities_ferc1, utilities_pudl):
    """Update the PUD: utility ID map with any missing FERC1 IDs."""
    util_ids_pudl_new = update_id_col(
        new_ids=utilities_ferc1, old_ids=utilities_pudl, id_col="utility_id_ferc1"
    ).pipe(autoincrement_from_max, id_col="utility_id_pudl")
    # update the ferc1 names based on the most recent xbrl name
    util_ids_ferc_w_names = utilities_ferc1.merge(
        get_util_ids_ferc1_raw_xbrl(),
        on=["utility_id_ferc1_xbrl"],
        how="inner",
        validate="m:1",
    )[["utility_id_ferc1", "utility_name_ferc1"]]

    util_ids_pudl_new = (
        utilities_pudl.merge(
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
