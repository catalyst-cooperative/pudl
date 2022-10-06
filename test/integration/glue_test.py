"""PyTest cases related to the integration between FERC1 & EIA 860/923."""
import logging
from collections.abc import Callable
from pathlib import Path

import pandas as pd
import pytest

from pudl.glue.ferc1_eia import (
    document_plant_eia_ids_for_manual_mapping,
    get_missing_ids,
    get_raw_plants_ferc1,
    get_unmapped_utils_eia,
    get_util_ids_ferc1_raw_xbrl,
    get_utils_ferc1_raw_dbf,
    glue,
)
from pudl.metadata.classes import DataSource
from pudl.output.pudltabl import PudlTabl

logger = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def pudl_out(pudl_engine, pudl_datastore_fixture):
    """A PUDL output object for use in CI."""
    return PudlTabl(
        pudl_engine,
        ds=pudl_datastore_fixture,
        freq=None,
    )


@pytest.fixture(scope="module")
def glue_test_dfs(
    pudl_out, ferc1_engine_xbrl, ferc1_engine_dbf, pudl_settings_fixture
) -> dict[str, pd.DataFrame]:
    """Make a dictionary of the dataframes required for this test module."""
    glue_test_dfs = {
        "plants_eia_pudl_db": pudl_out.plants_eia860(),
        "util_ids_ferc1_raw_xbrl": get_util_ids_ferc1_raw_xbrl(ferc1_engine_xbrl),
        "util_ids_ferc1_raw_dbf": get_utils_ferc1_raw_dbf(ferc1_engine_dbf),
        "plants_ferc1_raw": get_raw_plants_ferc1(
            pudl_settings=pudl_settings_fixture,
            years=DataSource.from_id("ferc1").working_partitions["years"],
        ),
    }
    glue_test_dfs.update(glue(eia=True, ferc1=True))
    return glue_test_dfs


def save_to_devtools_glue(df, test_dir, request):
    """Save a dataframe as a CSV to the glue directory in devtools."""
    file_path = Path(
        test_dir.parent,
        "devtools",
        "ferc1-eia-glue",
        f"{request.node.callspec.id}.csv",
    )
    df.to_csv(file_path)


ID_PARAMETERS = [
    pytest.param(
        "utilities_pudl",
        "utilities_ferc1",
        ["utility_id_pudl"],
        None,
        id="validate_utility_id_pudl_in_utilities_ferc1",
    ),
    pytest.param(
        "utilities_ferc1",
        "utilities_ferc1_dbf",
        ["utility_id_ferc1"],
        None,
        id="validate_utility_id_ferc1_in_utilities_ferc1_dbf",
    ),
    pytest.param(
        "utilities_ferc1",
        "utilities_ferc1_xbrl",
        ["utility_id_ferc1"],
        None,
        id="validate_utility_id_ferc1_in_utilities_ferc1_xbrl",
    ),
    pytest.param(
        "utilities_ferc1",
        "plants_ferc1",
        ["utility_id_ferc1"],
        None,
        id="validate_utility_id_ferc1_in_plants_ferc1",
    ),
    pytest.param(
        "utilities_ferc1_xbrl",
        "util_ids_ferc1_raw_xbrl",
        ["utility_id_ferc1_xbrl"],
        None,
        id="check_for_unmmaped_utility_id_ferc1_xbrl_in_raw_xbrl",
    ),
    pytest.param(
        "utilities_ferc1_dbf",
        "util_ids_ferc1_raw_dbf",
        ["utility_id_ferc1_dbf"],
        None,
        id="check_for_unmmaped_utility_id_ferc1_dbf_in_raw_dbf",
    ),
    pytest.param(
        "plants_pudl",
        "plants_ferc1",
        ["plant_id_pudl"],
        None,
        id="validate_plant_id_pudl_in_plants_ferc1",
    ),
    pytest.param(
        "plants_ferc1",
        "plants_ferc1_raw",
        ["utility_id_ferc1", "plant_name_ferc1"],
        None,
        id="check_for_unmmapped_plants_in_plants_ferc1",
    ),
    pytest.param(
        "plants_eia",
        "plants_eia_pudl_db",
        ["plant_id_eia"],
        document_plant_eia_ids_for_manual_mapping,
        id="check_for_unmmapped_plants_in_plants_eia",
    ),
]


@pytest.mark.parametrize("ids_left,ids_right,id_cols,label_func", ID_PARAMETERS)
def test_for_fk_validation_and_unmapped_ids(
    ids_left: str,
    ids_right: str,
    id_cols: list[str],
    label_func: Callable | None,
    glue_test_dfs: dict,
    pudl_out: PudlTabl,
    save_unmapped_ids: bool,
    test_dir,
    request,
):
    """Test that the stored ids are internally consistent. Label and save (optionally).

    Args:
        ids_left: name of fixure cooresponding to a dataframe which contains ID's
        ids_right: name of fixure cooresponding to a dataframe which contains ID's
        id_cols: list of ID column(s)
        label_func: If a labeling function is provided, label the missing ID's with
            flags and columns needed for manual mapping
        pudl_out: an instance of a pudl output object
        save_unmapped_ids: If ``True``, export any missing ID's.
        test_dir: path to the ``test`` directory. Will be used to construct path to the
            ``devtools/ferc1-eia-glue`` directory to save outputs into.

    Raises:
        AssertionError:
    """
    missing = get_missing_ids(
        glue_test_dfs[ids_left],
        glue_test_dfs[ids_right],
        id_cols,
    )
    if label_func:
        missing = label_func(missing, pudl_out)
    if save_unmapped_ids:
        save_to_devtools_glue(df=missing, test_dir=test_dir, request=request)
    if not missing.empty:
        raise AssertionError(f"Found {len(missing)} {id_cols}: {missing}")


@pytest.mark.parametrize(
    "ids_left,ids_right,id_cols,drop",
    [
        pytest.param(
            "plants_ferc1",
            "plants_ferc1_raw",
            ["utility_id_ferc1", "plant_name_ferc1"],
            (227, "comanche"),
            id="check_for_unmmapped_plants_in_plants_ferc1",
        ),
        pytest.param(
            "utilities_ferc1",
            "utilities_ferc1_xbrl",
            ["utility_id_ferc1"],
            (227),
            id="validate_utility_id_ferc1_in_utilities_ferc1_xbrl",
        ),
    ],
)
def test_for_unmapped_ids_minus_one(
    ids_left: str,
    ids_right: str,
    id_cols: list[str],
    drop: tuple,
    glue_test_dfs: dict[str, pd.DataFrame],
):
    """Test that we will find one unmapped ID after dropping one.

    Args:
        ids_left: name of fixure cooresponding to a dataframe which contains ID's
        ids_right: name of fixure cooresponding to a dataframe which contains ID's
        id_cols: list of ID column(s)
        drop: a tuple of the one record IDs to drop
        glue_test_dfs: dictionary of tables needed.

    Raises:
        AssertionError:
    """
    ids_minus_one = glue_test_dfs[ids_left].set_index(id_cols).drop(drop).reset_index()
    missing = get_missing_ids(ids_minus_one, glue_test_dfs[ids_right], id_cols)
    if len(missing) != 1:
        raise AssertionError(f"Found {len(missing)} {id_cols} but expected 1.")


def test_unmapped_utils_eia(
    pudl_out, pudl_engine, glue_test_dfs, save_unmapped_ids, test_dir, request
):
    """Check for unmapped EIA Plants.

    This test is duplicative with the sql foriegn key constraints.
    """
    unmapped_utils_eia = get_unmapped_utils_eia(
        pudl_out, pudl_engine, glue_test_dfs["utilities_eia"]
    )
    if save_unmapped_ids:
        save_to_devtools_glue(df=unmapped_utils_eia, test_dir=test_dir, request=request)
    if not unmapped_utils_eia.empty:
        raise AssertionError(
            f"Found {len(unmapped_utils_eia)} unmapped EIA utilities, expected 0."
            f"{unmapped_utils_eia}"
        )
