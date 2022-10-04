"""PyTest cases related to the integration between FERC1 & EIA 860/923."""
import logging

import pandas as pd
import pytest

from pudl.glue.ferc1_eia import (
    get_mapped_plants_ferc1,
    get_mapped_utils_ferc1,
    get_raw_plants_ferc1,
    get_unmapped_plants_eia,
    get_unmapped_plants_ferc1,
    get_unmapped_utils_eia,
    get_unmapped_utils_ferc1_dbf,
    glue,
)
from pudl.glue.xbrl_dbf_ferc1 import (
    get_util_ids_ferc1_raw_xbrl,
    get_utils_ferc1_raw_dbf,
)
from pudl.metadata.classes import DataSource

logger = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def glue_dfs() -> dict[str, pd.DataFrame]:
    """Make a dictionary of glue dataframes."""
    return glue(eia=True, ferc1=True)


@pytest.fixture(scope="module")
def utilities_pudl(glue_dfs) -> pd.DataFrame:
    """A table of PUDL utilities IDs."""
    return glue_dfs["utilities_pudl"]


@pytest.fixture(scope="module")
def utilities_ferc1(glue_dfs) -> pd.DataFrame:
    """A table of FERC1 utilities IDs."""
    return glue_dfs["utilities_ferc1"]


@pytest.fixture(scope="module")
def utilities_ferc1_xbrl(glue_dfs) -> pd.DataFrame:
    """A table of FERC1 XBRL utilities IDs."""
    return glue_dfs["utilities_ferc1_xbrl"]


@pytest.fixture(scope="module")
def utilities_ferc1_dbf(glue_dfs) -> pd.DataFrame:
    """A table of FERC1 DBF utilities IDs."""
    return glue_dfs["utilities_ferc1_dbf"]


@pytest.fixture(scope="module")
def util_ids_raw_xbrl(ferc1_xbrl_engine):
    """A fixure of utilty ids from the raw XBRL db."""
    return get_util_ids_ferc1_raw_xbrl(ferc1_xbrl_engine)


@pytest.fixture(scope="module")
def util_ids_raw_dbf(ferc1_dbf_engine):
    """A fixure of utilty ids from the raw XBRL db."""
    return get_utils_ferc1_raw_dbf(ferc1_dbf_engine)


def get_missing_ids(
    ids_left: pd.DataFrame,
    ids_right: pd.DataFrame,
    id_cols: list[str],
):
    """Identify IDs that are missing from the left df but show up in the right df."""
    id_test = pd.merge(ids_left, ids_right, on=id_cols, indicator=True)
    missing = id_test[id_test._merge == "right_only"]
    return missing


@pytest.mark.parametrize(
    "ids_left,ids_right,id_cols",
    [
        pytest.param(
            "utilities_pudl",
            "utilities_ferc1",
            ["utility_id_pudl"],
            id="validate_utility_id_pudl_in_utilities_ferc1",
        ),
        pytest.param(
            "utilities_ferc1",
            "utilities_ferc1_dbf",
            ["utility_id_ferc1"],
            id="validate_utility_id_ferc1_in_utilities_ferc1_dbf",
        ),
        pytest.param(
            "utilities_ferc1",
            "utilities_ferc1_xbrl",
            ["utility_id_ferc1"],
            id="validate_utility_id_ferc1_in_utilities_ferc1_xbrl",
        ),
        pytest.param(
            "utilities_ferc1_xbrl",
            "util_ids_raw_xbrl",
            ["utility_id_ferc1_xbrl"],
            id="check_for_unmmaped_utility_id_ferc1_xbrl_in_raw_xbrl",
        ),
        pytest.param(
            "utilities_ferc1_dbf",
            "util_ids_raw_dbf",
            ["utility_id_ferc1_dbf"],
            id="check_for_unmmaped_utility_id_ferc1_dbf_in_raw_dbf",
        ),
    ],
)
def test_for_util_ids_ferc1(
    ids_left: pd.DataFrame, ids_right: pd.DataFrame, id_cols: list[str], request
):
    """Test that the stored utility ids are internally consistent.

    Args:
        ids_left:
        ids_right:
        id_cols:

    Raises:
        AssertionError:
    """
    missing = get_missing_ids(
        request.getfixturevalue(ids_left), request.getfixturevalue(ids_right), id_cols
    )
    if not missing.empty:
        raise AssertionError(f"Found {len(missing)} {id_cols}: {missing}")


def test_unmapped_plants_ferc1(pudl_settings_fixture, ferc1_dbf_engine):
    """Test that we can correctly identify unmapped FERC Form 1 DB plants.

    This test replicates :func:`pudl.glue.ferc1_eia.get_unmapped_plants_ferc1`
    but deletes a plant from the raw FERC 1 DB contents, which should then be
    identified as "unmapped."
    """
    actually_unmapped_plants = get_unmapped_plants_ferc1(
        pudl_settings_fixture, DataSource.from_id("ferc1").working_partitions["years"]
    )
    if not actually_unmapped_plants.empty:
        raise AssertionError(
            f"Found {len(actually_unmapped_plants)} unmapped FERC 1 plants, "
            f"expected 0."
            f"{actually_unmapped_plants}"
        )

    # Get all the plants in the FERC 1 DB:
    db_plants = get_raw_plants_ferc1(
        pudl_settings_fixture, DataSource.from_id("ferc1").working_partitions["years"]
    ).set_index(["utility_id_ferc1", "plant_name_ferc1"])
    # Read in the mapped plants... but ditch Xcel's Comanche:
    mapped_plants = (
        get_mapped_plants_ferc1()
        .set_index(["utility_id_ferc1", "plant_name_ferc1"])
        .drop((227, "comanche"))
    )
    new_plants_index = db_plants.index.difference(mapped_plants.index)
    unmapped_plants = db_plants.loc[new_plants_index].reset_index()
    if len(unmapped_plants) != 1:
        raise AssertionError(
            f"Found {len(unmapped_plants)} unmapped FERC 1 plants instead of 1."
        )


@pytest.mark.xfail
def test_unmapped_utils_ferc1(pudl_settings_fixture, ferc1_dbf_engine):
    """Test that we can identify unmapped FERC 1 utilities."""
    # First run the unmapped utility function as is:
    actually_unmapped_utils = get_unmapped_utils_ferc1_dbf(ferc1_dbf_engine)
    if not actually_unmapped_utils.empty:
        raise AssertionError(
            f"Found {len(actually_unmapped_utils)} unmapped FERC 1 utilities, "
            f"expected 0.\n"
            f"{actually_unmapped_utils}"
        )
    logger.info("Found 0 unmapped FERC 1 utilities, as expected.")

    # Now do the smae thing... but yanking one of the mapped utils:
    mapped_utilities = (
        get_mapped_utils_ferc1()
        .set_index("utility_id_ferc1")
        .drop(227)  # Drop Xcel Energy Colorado / PSCo.
    )
    # Get all the plants in the FERC 1 DB:
    db_plants = get_raw_plants_ferc1(
        pudl_settings_fixture, DataSource.from_id("ferc1").working_partitions["years"]
    ).set_index(["utility_id_ferc1", "plant_name_ferc1"])
    # Read in the mapped plants... but ditch Xcel's Comanche:
    mapped_plants = (
        get_mapped_plants_ferc1()
        .set_index(["utility_id_ferc1", "plant_name_ferc1"])
        .drop((227, "comanche"))
    )
    new_plants_index = db_plants.index.difference(mapped_plants.index)
    unmapped_plants = db_plants.loc[new_plants_index].reset_index()
    # Generate a list of all utilities which have unmapped plants:
    # (Since any unmapped utility *must* have unmapped plants)
    utils_with_unmapped_plants = (
        unmapped_plants.loc[:, ["utility_id_ferc1", "utility_name_ferc1"]]
        .drop_duplicates("utility_id_ferc1")
        .set_index("utility_id_ferc1")
    )
    # Find the indices of all utilities with unmapped plants that do not appear
    # in the list of mapped utilities at all:
    new_utilities_index = utils_with_unmapped_plants.index.difference(
        mapped_utilities.index
    )
    # Use that index to select only the previously unmapped utilities:
    unmapped_utilities = utils_with_unmapped_plants.loc[
        new_utilities_index
    ].reset_index()
    if len(unmapped_utilities) != 1:
        raise AssertionError(
            f"Found {len(unmapped_utilities)} "
            f"unmapped FERC 1 utilities instead of 1."
        )


def test_unmapped_plants_eia(pudl_settings_fixture, pudl_engine):
    """Check for unmapped EIA Plants."""
    unmapped_plants_eia = get_unmapped_plants_eia(pudl_engine)
    if not unmapped_plants_eia.empty:
        raise AssertionError(
            f"Found {len(unmapped_plants_eia)} unmapped EIA plants, expected 0."
            f"{unmapped_plants_eia}"
        )


def test_unmapped_utils_eia(pudl_settings_fixture, pudl_engine):
    """Check for unmapped EIA Plants."""
    unmapped_utils_eia = get_unmapped_utils_eia(pudl_engine)
    if not unmapped_utils_eia.empty:
        raise AssertionError(
            f"Found {len(unmapped_utils_eia)} unmapped EIA utilities, expected 0."
            f"{unmapped_utils_eia}"
        )
