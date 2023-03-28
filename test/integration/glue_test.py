"""PyTest cases related to the integration between FERC1 & EIA 860/923."""
import logging
from pathlib import Path

import pandas as pd
import pytest

from pudl.glue.ferc1_eia import (
    get_missing_ids,
    get_plants_ferc1_raw_job,
    get_util_ids_eia_unmapped,
    get_util_ids_ferc1_raw_dbf,
    get_util_ids_ferc1_raw_xbrl,
    glue,
    label_missing_ids_for_manual_mapping,
    label_plants_eia,
    label_utilities_ferc1_dbf,
    label_utilities_ferc1_xbrl,
)
from pudl.output.pudltabl import PudlTabl

logger = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def pudl_out(pudl_engine):
    """A PUDL output object for use in CI."""
    return PudlTabl(
        pudl_engine,
        freq=None,
        fill_tech_desc=False,
        fill_net_gen=False,
    )


def plants_ferc1_raw(dataset_settings_config) -> pd.DataFrame:
    """Execute the partial ETL of FERC plant tables.

    Args:
        dataset_settings_config: dataset settings for the given pytest run.

    Returns:
        plants_ferc1_raw: all plants in the FERC Form 1 DBF and XBRL DB for given years.
    """
    result = get_plants_ferc1_raw_job().execute_in_process(
        run_config={
            "resources": {
                "dataset_settings": {
                    "config": dataset_settings_config,
                },
            }
        }
    )
    return result.output_for_node("plants_ferc1_raw")


@pytest.fixture(scope="module")
def glue_test_dfs(
    pudl_env,
    pudl_out,
    ferc1_engine_xbrl,
    ferc1_engine_dbf,
    pudl_settings_fixture,
    etl_settings,
    dataset_settings_config,
) -> dict[str, pd.DataFrame]:
    """Make a dictionary of the dataframes required for this test module."""
    glue_test_dfs = {
        "plants_eia_pudl_db": pudl_out.plants_eia860(),
        "util_ids_ferc1_raw_xbrl": get_util_ids_ferc1_raw_xbrl(ferc1_engine_xbrl),
        "util_ids_ferc1_raw_dbf": get_util_ids_ferc1_raw_dbf(ferc1_engine_dbf),
        "plants_ferc1_raw": plants_ferc1_raw(dataset_settings_config),
        "plants_eia_labeled": label_plants_eia(pudl_out),
    }
    glue_test_dfs.update(glue(eia=True, ferc1=True))
    # more glue test input tables that are compiled from tables already in glue_test_dfs
    glue_test_dfs.update(
        {
            "utilities_ferc1_dbf_labeled": label_utilities_ferc1_dbf(
                glue_test_dfs["utilities_ferc1_dbf"],
                glue_test_dfs["util_ids_ferc1_raw_dbf"],
            ),
            "utilities_ferc1_xbrl_labeled": label_utilities_ferc1_xbrl(
                glue_test_dfs["utilities_ferc1_xbrl"],
                glue_test_dfs["util_ids_ferc1_raw_xbrl"],
            ),
        }
    )
    return glue_test_dfs


def save_to_devtools_glue(missing_df: pd.DataFrame, test_dir, file_name: str):
    """Save a dataframe as a CSV to the glue directory in devtools."""
    file_path = Path(test_dir.parent, "devtools", "ferc1-eia-glue", file_name)
    missing_df.to_csv(file_path)


@pytest.mark.parametrize(
    "ids_left,ids_right,id_cols,label_df",
    [
        pytest.param(
            "utilities_pudl",
            "utilities_ferc1",
            ["utility_id_pudl"],
            "utilities_ferc1",
            id="missing_utility_id_pudl_in_utilities_ferc1",
        ),
        pytest.param(
            "utilities_ferc1",
            "utilities_ferc1_dbf",
            ["utility_id_ferc1"],
            "utilities_ferc1_dbf_labeled",
            id="missing_utility_id_ferc1_in_utilities_ferc1_dbf",
        ),
        pytest.param(
            "utilities_ferc1",
            "utilities_ferc1_xbrl",
            ["utility_id_ferc1"],
            "utilities_ferc1_xbrl_labeled",
            id="missing_utility_id_ferc1_in_utilities_ferc1_xbrl",
        ),
        pytest.param(
            "utilities_ferc1",
            "plants_ferc1",
            ["utility_id_ferc1"],
            "plants_ferc1",
            id="missing_utility_id_ferc1_in_plants_ferc1",
        ),
        pytest.param(
            "utilities_ferc1_xbrl",
            "util_ids_ferc1_raw_xbrl",
            ["utility_id_ferc1_xbrl"],
            "util_ids_ferc1_raw_xbrl",
            id="missing_utility_id_ferc1_xbrl_in_raw_xbrl",
        ),
        pytest.param(
            "utilities_ferc1_dbf",
            "util_ids_ferc1_raw_dbf",
            ["utility_id_ferc1_dbf"],
            "util_ids_ferc1_raw_dbf",
            id="missing_utility_id_ferc1_dbf_in_raw_dbf",
        ),
        pytest.param(
            "plants_pudl",
            "plants_ferc1",
            ["plant_id_pudl"],
            None,  # should only ever happen if a plant is in the mapping sheet w/o a pudl id
            id="missing_plant_id_pudl_in_plants_ferc1",
        ),
        pytest.param(
            "plants_ferc1",
            "plants_ferc1_raw",
            ["utility_id_ferc1", "plant_name_ferc1"],
            "plants_ferc1_raw",
            id="missing_plants_in_plants_ferc1",
        ),
        pytest.param(
            "plants_eia",
            "plants_eia_pudl_db",
            ["plant_id_eia"],
            "plants_eia_labeled",
            id="missing_plants_in_plants_eia",
        ),
    ],
)
def test_for_fk_validation_and_unmapped_ids(
    ids_left: str,
    ids_right: str,
    id_cols: list[str],
    label_df: pd.DataFrame | None,
    glue_test_dfs: dict[str, pd.DataFrame],
    save_unmapped_ids: bool,
    test_dir,
    request,
):
    """Test that the stored ids are internally consistent. Label and save (optionally).

    Args:
        ids_left: name of key to access corresponding to a dataframe which contains
            ID's in ``glue_test_dfs``
        ids_right: name of key to access corresponding to a dataframe which contains
            ID's in ``glue_test_dfs``
        id_cols: list of ID column(s)
        label_df: If a labeling table is provided, label the missing ID's with flags
            and columns needed for manual mapping
        pudl_out: an instance of a pudl output object
        glue_test_dfs: a dictionary of dataframes
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
    if label_df:
        missing_df = label_missing_ids_for_manual_mapping(
            missing, glue_test_dfs[label_df]
        )
    else:
        missing_df = pd.DataFrame(index=missing)
    if save_unmapped_ids:
        save_to_devtools_glue(
            missing_df=missing_df,
            test_dir=test_dir,
            file_name=f"{request.node.callspec.id}.csv",
        )
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
        ids_left: name of key to access corresponding to a dataframe which contains
            ID's in ``glue_test_dfs``
        ids_right: name of key to access corresponding to a dataframe which contains
            ID's in ``glue_test_dfs``
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
    pudl_out, pudl_engine, glue_test_dfs, save_unmapped_ids, test_dir
):
    """Check for unmapped EIA Plants.

    This test has its own call signature because its more complex. In order to label the
    missing utilities, we use both the ``pudl_out`` object as well as direct SQL
    queries. This test is duplicative with the sql foriegn key constraints.
    """
    unmapped_utils_eia = get_util_ids_eia_unmapped(
        pudl_out, pudl_engine, glue_test_dfs["utilities_eia"]
    )
    if save_unmapped_ids:
        save_to_devtools_glue(
            missing_df=unmapped_utils_eia,
            test_dir=test_dir,
            file_name="missing_utility_id_eia_in_utilities_eia.csv",
        )
    if not unmapped_utils_eia.empty:
        raise AssertionError(
            f"Found {len(unmapped_utils_eia)} unmapped EIA utilities, expected 0."
            f"{unmapped_utils_eia}"
        )
