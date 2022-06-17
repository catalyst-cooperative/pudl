"""Extract, clean, and normalize the EPA-EIA crosswalk.

This module defines functions that read the raw EPA-EIA crosswalk file, clean
up the column names, and separate it into three distinctive normalize tables
for integration in the database.

The crosswalk file was a joint effort on behalf on EPA and EIA and is published on the
EPA's github account at www.github.com/USEPA".
"""
import logging

import pandas as pd

import pudl
from pudl.metadata.fields import apply_pudl_dtypes
from pudl.workspace.datastore import Datastore

logger = logging.getLogger(__name__)


def extract(ds: Datastore) -> pd.DataFrame:
    """Extract the EPACEMS-EIA Crosswalk from the Datastore."""
    with ds.get_zipfile_resource(
        "epacems_unitid_eia_plant_crosswalk",
        name="epacems_unitid_eia_plant_crosswalk.zip",
    ).open("camd-eia-crosswalk-master/epa_eia_crosswalk.csv") as f:
        return pd.read_csv(f)


def transform(epa_eia_crosswalk: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Clean up the EPACEMS-EIA Crosswalk file and split it into normalized tables.

    Args:
        epa_eia_crosswalk: The result of running this module's extract() function.

    Returns:
        A dictionary of three normalized DataFrames comprised of the data
        in the original crosswalk file. EPA plant id to EPA unit id; EPA plant
        id to EIA plant id; and EIA plant id to EIA generator id to EPA unit
        id. Includes no nan values.
    """
    logger.info("Cleaning up the epacems-eia crosswalk")

    column_rename = {
        "camd_unit_id": "unit_id_epa",
        "camd_plant_id": "plant_id_epa",
        "eia_plant_name": "plant_name_eia",
        "eia_plant_id": "plant_id_eia",
        "eia_generator_id": "generator_id",
    }
    crosswalk_clean = (
        epa_eia_crosswalk.pipe(pudl.helpers.simplify_columns)
        .rename(columns=column_rename)
        .filter(list(column_rename.values()))
        .pipe(apply_pudl_dtypes, "eia")
    )

    logger.info("Splitting crosswalk into three normalized tables")

    def drop_n_reset(df, cols):
        return df.filter(cols).copy().dropna()

    epa_df = drop_n_reset(crosswalk_clean, ["plant_id_epa", "unit_id_epa"])
    plants_eia_epa_df = drop_n_reset(crosswalk_clean, ["plant_id_eia", "plant_id_epa"])
    gen_unit_df = drop_n_reset(
        crosswalk_clean, ["plant_id_eia", "generator_id", "unit_id_epa"]
    )

    return {
        "plant_unit_epa": epa_df,
        "assn_plant_id_eia_epa": plants_eia_epa_df,
        "assn_gen_eia_unit_epa": gen_unit_df,
    }


def grab_clean_split(ds: Datastore) -> dict[str, pd.DataFrame]:
    """Clean raw crosswalk data, drop nans, and return split tables.

    Args:
        ds (:class:datastore.Datastore): Initialized datastore.

    Returns:
        A dictionary of three normalized DataFrames comprised of the data
        in the original crosswalk file. EPA plant id to EPA unit id; EPA plant
        id to EIA plant id; and EIA plant id to EIA generator id to EPA unit
        id.
    """
    return transform(extract(ds))
