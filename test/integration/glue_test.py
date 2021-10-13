"""PyTest cases related to the integration between FERC1 & EIA 860/923."""
import logging

import pandas as pd

import pudl
from pudl import constants as pc

logger = logging.getLogger(__name__)


def test_unmapped_plants_ferc1(pudl_settings_fixture, ferc1_engine):
    """
    Test that we can correctly identify unmapped FERC Form 1 DB plants.

    This test replicates :func:`pudl.glue.ferc1_eia.get_unmapped_plants_ferc1`
    but deletes a plant from the raw FERC 1 DB contents, which should then be
    identified as "unmapped."

    """
    actually_unmapped_plants = (
        pudl.glue.ferc1_eia.
        get_unmapped_plants_ferc1(pudl_settings_fixture,
                                  pc.WORKING_PARTITIONS['ferc1']['years'])
    )
    if len(actually_unmapped_plants) != 0:
        raise AssertionError(
            f"Expected zero unmapped FERC 1 plants but found "
            f"{len(actually_unmapped_plants)}"
        )
    logger.info("Found 0 unmapped FERC 1 plants, as expected.")
    # Get all the plants in the FERC 1 DB:
    db_plants = (
        pudl.glue.ferc1_eia.
        get_db_plants_ferc1(pudl_settings_fixture,
                            pc.WORKING_PARTITIONS['ferc1']['years']).
        set_index(["utility_id_ferc1", "plant_name_ferc1"])
    )
    # Read in the mapped plants... but ditch Xcel's Comanche:
    mapped_plants = (
        pudl.glue.ferc1_eia.get_mapped_plants_ferc1().
        set_index(["utility_id_ferc1", "plant_name_ferc1"]).
        drop((145, "comanche"))
    )
    new_plants_index = db_plants.index.difference(mapped_plants.index)
    unmapped_plants = db_plants.loc[new_plants_index].reset_index()
    if len(unmapped_plants) != 1:
        raise AssertionError(
            f"Found {len(unmapped_plants)} "
            f"unmapped FERC 1 plants instead of 1."
        )
    logger.info("Found 1 unmapped FERC 1 plant, as expected.")


def test_unmapped_utils_ferc1(pudl_settings_fixture, ferc1_engine):
    """Test that we can identify unmapped FERC 1 utilities."""
    # First run the unmapped utility function as is:
    actually_unmapped_utils = (
        pudl.glue.ferc1_eia.
        get_unmapped_utils_ferc1(ferc1_engine)
    )
    if len(actually_unmapped_utils) != 0:
        raise AssertionError(
            f"Expected zero unmapped FERC 1 utilities but found "
            f"{len(actually_unmapped_utils)}. \n {actually_unmapped_utils}"
        )
    logger.info("Found 0 unmapped FERC 1 utilities, as expected.")

    # Now do the smae thing... but yanking one of the mapped utils:
    mapped_utilities = (
        pudl.glue.ferc1_eia.
        get_mapped_utils_ferc1().
        set_index("utility_id_ferc1").
        drop(145)  # Drop Xcel Energy Colorado / PSCo.
    )
    # Get all the plants in the FERC 1 DB:
    db_plants = (
        pudl.glue.ferc1_eia.
        get_db_plants_ferc1(pudl_settings_fixture,
                            pc.WORKING_PARTITIONS['ferc1']['years']).
        set_index(["utility_id_ferc1", "plant_name_ferc1"])
    )
    # Read in the mapped plants... but ditch Xcel's Comanche:
    mapped_plants = (
        pudl.glue.ferc1_eia.get_mapped_plants_ferc1().
        set_index(["utility_id_ferc1", "plant_name_ferc1"]).
        drop((145, "comanche"))
    )
    new_plants_index = db_plants.index.difference(mapped_plants.index)
    unmapped_plants = db_plants.loc[new_plants_index].reset_index()
    # Generate a list of all utilities which have unmapped plants:
    # (Since any unmapped utility *must* have unmapped plants)
    utils_with_unmapped_plants = (
        unmapped_plants.loc[:, ["utility_id_ferc1", "utility_name_ferc1"]].
        drop_duplicates("utility_id_ferc1").
        set_index("utility_id_ferc1")
    )
    # Find the indices of all utilities with unmapped plants that do not appear
    # in the list of mapped utilities at all:
    new_utilities_index = (
        utils_with_unmapped_plants.index.
        difference(mapped_utilities.index)
    )
    # Use that index to select only the previously unmapped utilities:
    unmapped_utilities = (
        utils_with_unmapped_plants.
        loc[new_utilities_index].
        reset_index()
    )
    if len(unmapped_utilities) != 1:
        raise AssertionError(
            f"Found {len(unmapped_utilities)} "
            f"unmapped FERC 1 utilities instead of 1."
        )
    logger.info("Found 1 unmapped FERC 1 utility, as expected.")


def test_unmapped_plants_eia(pudl_settings_fixture, pudl_engine):
    """Check for unmapped EIA Plants."""
    unmapped_plants_eia = pudl.glue.ferc1_eia.get_unmapped_plants_eia(
        pudl_engine)
    if len(unmapped_plants_eia) > 0:
        raise AssertionError(
            f"Found {len(unmapped_plants_eia)} unmapped EIA plants. Expected 0."
        )


def test_unmapped_utils_eia(pudl_settings_fixture, pudl_datastore_fixture, pudl_engine):
    """
    Check for unmapped EIA Utilities.

    The EIA 860 contains thousands of utility IDs, most of which do not have
    any data associated with them in the EIA 923, even if they do have plants
    associated with them in the EIA 860. In practice the only utilities which
    we have been making sure have PUDL IDs are the ones that show up in the
    EIA 923, so those tables are the only ones we're searching here for utility
    IDs.

    """
    pudl_raw = pudl.output.pudltabl.PudlTabl(
        pudl_engine,
        ds=pudl_datastore_fixture,
        freq=None
    )
    frc_eia923 = pudl_raw.frc_eia923()
    gf_eia923 = pudl_raw.gf_eia923()
    gen_eia923 = pudl_raw.gen_eia923()
    bf_eia923 = pudl_raw.bf_eia923()

    cols = ["utility_id_eia", "utility_name_eia"]
    missing_frc = frc_eia923[frc_eia923.utility_id_pudl.isna()][cols]
    missing_gf = gf_eia923[gf_eia923.utility_id_pudl.isna()][cols]
    missing_bf = bf_eia923[bf_eia923.utility_id_pudl.isna()][cols]
    missing_gens = gen_eia923[gen_eia923.utility_id_pudl.isna()][cols]

    missing_utils = (
        pd.concat([missing_frc, missing_bf, missing_gf, missing_gens])
        .drop_duplicates(subset="utility_id_eia")
        .set_index("utility_id_eia")
    )
    if len(missing_utils) > 0:
        raise AssertionError(
            f"Found {len(missing_utils)} unmapped EIA utilities that"
            "report data in the EIA 923. Expected zero."
        )
