"""Tests for the validation functions in the eia_ferc1_train module.

The functions that this module is testing were not designed with tests in mind. Ideally
we would refactor the functions to be more testable, but for now we are just focusing on
getting them tested.

These tests also struggle with the need to have parameters with data from the most
recent year (so as not to fail during the fast_etl). In future years, some of these will
xfail for the wrong reasons (because they are from a year that doesn't exist in the fast
data not because of the stated reason). We'll need to fix both the xfail and non xfail
parameters. Ideally we'll design a way to automatically grab the most recent year so we
don't have to update this manually.
"""

import importlib.resources
import logging
from contextlib import nullcontext
from pathlib import Path

import pandas as pd
import pytest
import sqlalchemy as sa

from pudl.analysis.record_linkage.eia_ferc1_inputs import (
    restrict_train_connections_on_date_range,
)
from pudl.analysis.record_linkage.eia_ferc1_train import (
    generate_all_override_spreadsheets,
    validate_override_fixes,
)
from pudl.helpers import get_parquet_table

logger = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def eia_ferc1_training_data() -> pd.DataFrame:
    """The training data for the eia_ferc1 matching."""
    return pd.read_csv(
        importlib.resources.files("pudl.package_data.glue") / "eia_ferc1_train.csv"
    )


@pytest.mark.parametrize(
    "verified,report_year,record_id_eia_override_1,record_id_ferc1,utility_id_pudl_ferc1,expectation",
    [
        # This param will need to be updated with data from new years in order to pass
        # None of these parameters represent real matches. They mimic real matches by
        # coming from the same year and utility.
        pytest.param(
            ["True"],
            [2020],
            ["2_2020_plant_owned_195"],
            ["f1_steam_2020_12_2_0_1"],
            [18],
            nullcontext(),
        ),
        pytest.param(
            ["True"],
            [2019],
            ["2_2020_plant_owned_195"],
            ["f1_steam_2020_12_2_0_1"],
            [18],
            pytest.raises(
                AssertionError,
                match=r"Found record_id_eia_override_1 values that don't correspond",
            ),
        ),
        pytest.param(
            ["True", "True"],
            [2020, 2020],
            ["2_2020_plant_owned_195", "2_2020_plant_owned_195"],
            ["f1_steam_2020_12_2_0_1", "f1_steam_2020_12_2_0_2"],
            [18, 18],
            pytest.raises(
                AssertionError, match=r"Found record_id_eia_override_1 duplicates"
            ),
        ),
        pytest.param(
            ["True", "True"],
            [2020, 2020],
            ["2_2020_plant_owned_195", "3_2020_plant_owned_195"],
            ["f1_steam_2020_12_2_0_1", "f1_steam_2020_12_2_0_1"],
            [18, 18],
            pytest.raises(AssertionError, match=r"Found record_id_ferc1 duplicates"),
        ),
        pytest.param(
            ["True"],
            [2020],
            ["299_2020_plant_total_14354"],  # EIA id already in training data
            ["f1_steam_2020_12_134_2_2"],  # FERC1 id NOT in training data
            [246],
            pytest.raises(
                AssertionError,
                match=r"The following EIA records are already in the training data",
            ),
        ),
        pytest.param(
            ["True"],
            [2020],
            ["294_2020_plant_owned_14354"],  # EIA id NOT in training data
            ["f1_steam_2020_12_134_3_1"],  # FERC1 id already in training data
            [246],
            pytest.raises(
                AssertionError,
                match=r"The following FERC 1 records are already in the training data",
            ),
        ),
        pytest.param(
            ["True"],
            [2020],
            ["1_2020_plant_owned_63560"],
            ["f1_steam_2020_12_161_0_1"],
            [1],
            pytest.raises(AssertionError, match=r"Found mismatched utilities"),
        ),
    ],
)
def test_validate_override_fixes(
    eia_ferc1_training_data: pd.DataFrame,
    verified: list[str],
    report_year: list[int],
    record_id_eia_override_1: list[str],
    record_id_ferc1: list[str],
    utility_id_pudl_ferc1: list[int],
    expectation,
    pudl_engine: sa.Engine,  # Required to ensure that the data is available.
) -> None:
    """Test the validate override fixes function."""
    # Get data tables with only the columns needed by validate_override_fixes
    # to reduce memory usage during testing
    plant_parts_eia = get_parquet_table(
        "out_eia__yearly_plant_parts",
        columns=["record_id_eia", "utility_id_pudl", "report_year"],
    )
    eia_ferc1 = get_parquet_table(
        "out_pudl__yearly_assn_eia_ferc1_plant_parts",
        columns=["record_id_ferc1", "report_date"],
    )

    test_df = pd.DataFrame(
        {
            "verified": verified,
            "report_year": report_year,
            "record_id_eia_override_1": record_id_eia_override_1,
            "record_id_ferc1": record_id_ferc1,
            "utility_id_pudl_ferc1": utility_id_pudl_ferc1,
        }
    ).assign(verified=lambda x: x.verified.astype("bool"))
    eia_ferc1_training_data_restricted = restrict_train_connections_on_date_range(
        train_df=eia_ferc1_training_data,
        id_col="record_id_ferc1",
        start_date=min(eia_ferc1.report_date),
        end_date=max(eia_ferc1.report_date),
    )
    with expectation:
        validate_override_fixes(
            validated_connections=test_df,
            ppe=plant_parts_eia,
            eia_ferc1=eia_ferc1,
            training_data=eia_ferc1_training_data_restricted,
            expect_override_overrides=False,
            allow_mismatched_utilities=False,
        )


def test_generate_all_override_spreadsheets(
    pudl_engine: sa.Engine,  # Required to ensure that the data is available.
):
    """Test the genation of the override spreadsheet for mapping FERC-EIA records."""
    # Get data tables directly
    plant_parts_eia = get_parquet_table("out_eia__yearly_plant_parts")
    eia_ferc1 = get_parquet_table("out_pudl__yearly_assn_eia_ferc1_plant_parts")
    utils_eia860 = get_parquet_table("out_eia__yearly_utilities")

    # Create the test spreadsheet
    generate_all_override_spreadsheets(
        eia_ferc1,
        plant_parts_eia,
        utils_eia860,
        util_dict={"NextEra": [6452, 7801]},
        years=[2020],
        output_dir_path=f"{Path.cwd()}",
    )
    # Make sure there is something there
    mapping_spreadsheet = pd.read_excel(
        f"{Path.cwd()}/NextEra_fix_FERC-EIA_overrides.xlsx",
        engine="calamine",
    )
    if mapping_spreadsheet.empty:
        raise AssertionError("Mapping spreadsheet has no contents")

    # Remove this test file
    Path.unlink(f"{Path.cwd()}/NextEra_fix_FERC-EIA_overrides.xlsx")
