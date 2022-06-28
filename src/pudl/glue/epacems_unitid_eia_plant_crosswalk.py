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

# from typing import Boolean
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


def transform(
    epa_eia_crosswalk: pd.DataFrame,
    generators_entity_eia: pd.DataFrame,
    processing_all_eia_years: bool,
) -> dict[str, pd.DataFrame]:
    """Clean up the EPACEMS-EIA Crosswalk file and split it into normalized tables.

    Args:
        epa_eia_crosswalk: The result of running this module's extract() function.
        generators_entity_eia: The generators_entity_eia table.
        processing_all_years: A boolean indicating whether the years from the
            Eia860Settings object match the EIA860 working partitions. This indicates
            whether or not to restrict the crosswalk data so the tests don't fail on
            foreign key restraints.

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

    # Basic column rename, selection, and dtype alignment.
    crosswalk_clean = (
        epa_eia_crosswalk.pipe(pudl.helpers.simplify_columns)
        .rename(columns=column_rename)
        .filter(list(column_rename.values()))
        .pipe(apply_pudl_dtypes, "eia")
    )

    # The crosswalk is a static file: there is no year field. The plant_id_eia and
    # generator_id fields, however, are foreign keys from an annualized table. If the
    # fast ETL is run (on one year of data) the test will break because the crosswalk
    # tables with plant_id_eia and generator_id contain values from various years. To
    # keep the crosswalk in alignment with the available eia data, we'll restrict it
    # based on the generator entity table which has plant id and generator id so long
    # as it's not using the full suite of avilable years. If it is, we don't want to
    # restrict the crosswalk so we can get warnings and errors from any foreign key
    # discrepancies.
    if not processing_all_eia_years:
        logger.info(
            "Selected subset of avilable EIA years--restricting EIA-EPA Crosswalk to \
            chosen subset of EIA years"
        )
        crosswalk_clean = pd.merge(
            crosswalk_clean.dropna(subset=["plant_id_eia"]),
            generators_entity_eia[["plant_id_eia", "generator_id"]].drop_duplicates(),
            on=["plant_id_eia", "generator_id"],
            how="inner",
        )

    # There are some eia generator_id values in the crosswalk that don't match the eia
    # generator_id values in the generators_eia860 table where the foreign keys are
    # stored. All of them appear to have preceeding zeros. I.e.: 0010 should be 10.
    # This makes sure to nix preceeding zeros on crosswalk generator ids that are all
    # numeric. I.e.: 00A10 will stay 00A10 but 0010 will become 10. This same method
    # is applied to the EIA data.
    crosswalk_clean = pudl.helpers.fix_leading_zero_gen_ids(crosswalk_clean)

    # NOTE: still need to see whether unit_id matches up with the values in EPA well!

    logger.info("Splitting crosswalk into three normalized tables")

    def drop_n_reset(df, cols):
        return df.filter(cols).copy().dropna().drop_duplicates()

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


def crosswalk_et(
    ds: Datastore, generators_entity_eia: pd.DataFrame, processing_all_eia_years: bool
) -> dict[str, pd.DataFrame]:
    """Clean raw crosswalk data, drop nans, and return split tables.

    Args:
        ds: Initialized datastore.
        generators_entity_eia: The generators_entity_eia table.
        processing_all_eia_years: A boolean indicating whether the years from the
            Eia860Settings object match the EIA860 working partitions. This tell the
            function whether to restrict the crosswalk data so the tests don't fail on
            foreign key restraints.

    Returns:
        A dictionary of three normalized DataFrames comprised of the data
        in the original crosswalk file. EPA plant id to EPA unit id; EPA plant
        id to EIA plant id; and EIA plant id to EIA generator id to EPA unit
        id.
    """
    return transform(extract(ds), generators_entity_eia, processing_all_eia_years)
