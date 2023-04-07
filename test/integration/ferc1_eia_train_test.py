"""Tests for the validation functions in the ferc1_eia_train module.

The functions that this module is testing were not designed with tests in mind. Ideally
we would refactor the functions to be more testable, but for now we are just focusing on
getting them tested.

These tests also struggle with the need to have parameters with data from the most
recent year (so as not to fail during the fast_etl). In future years, some of these
will xfail for the wrong reasons (because they are from a year that doesn't exist in
the fast data not because of the stated reason). We'll need to fix both the xfail and
non xfail parameters. Ideally we'll design a way to automatically grab the most recent
year so we don't have to update this manually.
"""

import importlib.resources
import logging
import os

import pandas as pd
import pytest

from pudl.analysis.ferc1_eia import restrict_train_connections_on_date_range
from pudl.analysis.ferc1_eia_train import (
    generate_all_override_spreadsheets,
    validate_override_fixes,
)

logger = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def utils_eia860(fast_out_annual):
    """The utils_eia860 output table."""
    return fast_out_annual.utils_eia860()


@pytest.fixture(scope="module")
def plant_parts_eia(fast_out_annual):
    """The plant_parts_eia output table."""
    return fast_out_annual.plant_parts_eia().reset_index()


@pytest.fixture(scope="module")
def ferc1_eia(fast_out_annual):
    """The ferc1_eia output table."""
    return fast_out_annual.ferc1_eia()


@pytest.fixture(scope="module")
def ferc1_eia_training_data():
    """The training data for the ferc1_eia matching."""
    return pd.read_csv(
        importlib.resources.open_text("pudl.package_data.glue", "ferc1_eia_train.csv")
    )


@pytest.mark.parametrize(
    "verified,report_year,record_id_eia_override_1,record_id_ferc1",
    [
        pytest.param(
            ["True"],
            [2020],
            ["4270_2020_plant_total_11241"],
            ["f1_steam_2020_12_454_0_3"],
        ),
        # This param will need to be upated with data from new years
        pytest.param(
            ["True"],
            [2019],
            ["4270_2020_plant_total_11241"],
            ["f1_steam_2020_12_454_0_3"],
            marks=pytest.mark.xfail(
                reason="EIA record year doesn't match FERC record year",
                raises=AssertionError,
            ),
        ),
        pytest.param(
            ["True", "True"],
            [2020, 2020],
            ["4270_2020_plant_total_11241", "4270_2020_plant_total_11241"],
            ["f1_steam_2020_12_454_0_3", "f1_steam_2020_12_159_0_3"],
            marks=pytest.mark.xfail(
                reason="Duplicate EIA ids in training data", raises=AssertionError
            ),
        ),
        pytest.param(
            ["True", "True"],
            [2020, 2020],
            ["4270_2020_plant_total_11241", "4271_2020_plant_total_11241"],
            ["f1_steam_2020_12_454_0_3", "f1_steam_2020_12_454_0_3"],
            marks=pytest.mark.xfail(
                reason="Duplicate FERC1 ids in training data", raises=AssertionError
            ),
        ),
    ],
)
def test_validate_override_fixes(
    utils_eia860,
    plant_parts_eia,
    ferc1_eia,
    ferc1_eia_training_data,
    verified,
    report_year,
    record_id_eia_override_1,
    record_id_ferc1,
):
    """Test the validate override fixes function."""
    test_df = pd.DataFrame(
        {
            "verified": verified,
            "report_year": report_year,
            "record_id_eia_override_1": record_id_eia_override_1,
            "record_id_ferc1": record_id_ferc1,
        }
    ).assign(verified=lambda x: x.verified.astype("bool"))
    ferc1_eia_training_data_restricted = restrict_train_connections_on_date_range(
        train_df=ferc1_eia_training_data,
        id_col="record_id_ferc1",
        start_date=min(ferc1_eia.report_date),
        end_date=max(ferc1_eia.report_date),
    )
    validate_override_fixes(
        validated_connections=test_df,
        utils_eia860=utils_eia860,
        ppe=plant_parts_eia,
        ferc1_eia=ferc1_eia,
        training_data=ferc1_eia_training_data_restricted,
        expect_override_overrides=True,
        allow_mismatched_utilities=True,
    )


@pytest.mark.parametrize(
    "verified,report_year,record_id_eia_override_1,record_id_ferc1",
    [
        pytest.param(
            ["True"],
            [2020],
            # NOTE: These are not matches, just test examples
            ["1_2020_plant_owned_63560"],  # EIA id NOT in training data.
            ["f1_steam_2020_12_161_0_1"],  # FERC1 id NOT in training data.
        ),
        pytest.param(
            ["True"],
            [2020],
            ["299_2020_plant_total_14354"],
            ["f1_steam_2020_12_134_3_1"],
            marks=pytest.mark.xfail(
                reason="FERC1 and EIA ids already in training data."
            ),
        ),
        pytest.param(
            ["True"],
            [2020],
            # NOTE: These are not matches, just test examples
            ["299_2020_plant_total_14354"],  # EIA id already in training data
            ["f1_steam_2020_12_161_0_1"],  # FERC1 id NOT in training data
            marks=pytest.mark.xfail(reason="EIA id already in training data."),
        ),
        pytest.param(
            ["True"],
            [2020],
            # NOTE: These are not matches, just test examples
            ["1_2020_plant_owned_63560"],  # EIA id NOT in training data
            ["f1_steam_2020_12_134_3_1"],  # FERC1 id already in training data
            marks=pytest.mark.xfail(reason="FERC1 id already in training data."),
        ),
    ],
)
def test_validate_override_fixes_no_overrides(
    utils_eia860,
    plant_parts_eia,
    ferc1_eia,
    ferc1_eia_training_data,
    verified,
    report_year,
    record_id_eia_override_1,
    record_id_ferc1,
):
    """Test validate override fixes function where expect_override_overrides=False."""
    test_df = pd.DataFrame(
        {
            "verified": verified,
            "report_year": report_year,
            "record_id_eia_override_1": record_id_eia_override_1,
            "record_id_ferc1": record_id_ferc1,
        }
    ).assign(verified=lambda x: x.verified.astype("bool"))
    ferc1_eia_training_data_restricted = restrict_train_connections_on_date_range(
        train_df=ferc1_eia_training_data,
        id_col="record_id_ferc1",
        start_date=min(ferc1_eia.report_date),
        end_date=max(ferc1_eia.report_date),
    )
    validate_override_fixes(
        validated_connections=test_df,
        utils_eia860=utils_eia860,
        ppe=plant_parts_eia,
        ferc1_eia=ferc1_eia,
        training_data=ferc1_eia_training_data_restricted,
        expect_override_overrides=False,
        allow_mismatched_utilities=True,
    )


@pytest.mark.parametrize(
    "verified,report_year,record_id_eia_override_1,record_id_ferc1,utility_id_eia,utility_id_pudl",
    [
        pytest.param(
            ["True"],
            [2020],
            # NOTE: These are not matches, just test examples
            ["1_2020_plant_owned_63560"],
            ["f1_steam_2020_12_161_0_1"],
            [63560],
            [1],
            marks=pytest.mark.xfail(reason="Utilities don't match"),
        ),
        pytest.param(
            ["True"],
            [2020],
            ["8055_2020_plant_total_814"],
            ["f1_steam_2020_12_8_0_5"],
            [814],
            [106],
        ),
    ],
)
def test_validate_override_fixes_no_mismatched_utilities(
    utils_eia860,
    plant_parts_eia,
    ferc1_eia,
    ferc1_eia_training_data,
    verified,
    report_year,
    record_id_eia_override_1,
    record_id_ferc1,
    utility_id_eia,
    utility_id_pudl,
):
    """Test validate override fixes function where mismatched_utilities=False.

    The parameters have a field for utility_id_eia and utility_id_pudl because the
    override spreadsheets also have that field. As written, the
    """
    test_df = pd.DataFrame(
        {
            "verified": verified,
            "report_year": report_year,
            "record_id_eia_override_1": record_id_eia_override_1,
            "record_id_ferc1": record_id_ferc1,
            "utility_id_eia": utility_id_eia,
            "utility_id_pudl": utility_id_pudl,
        }
    ).assign(verified=lambda x: x.verified.astype("bool"))
    ferc1_eia_training_data_restricted = restrict_train_connections_on_date_range(
        train_df=ferc1_eia_training_data,
        id_col="record_id_ferc1",
        start_date=min(ferc1_eia.report_date),
        end_date=max(ferc1_eia.report_date),
    )
    validate_override_fixes(
        validated_connections=test_df,
        utils_eia860=utils_eia860,
        ppe=plant_parts_eia,
        ferc1_eia=ferc1_eia,
        training_data=ferc1_eia_training_data_restricted,
        expect_override_overrides=True,
        allow_mismatched_utilities=False,
    )


def test_generate_all_override_spreadsheets(plant_parts_eia, ferc1_eia, utils_eia860):
    """Test the genation of the override spreadsheet for mapping FERC-EIA records."""
    # Create the test spreadsheet
    generate_all_override_spreadsheets(
        ferc1_eia,
        plant_parts_eia,
        utils_eia860,
        util_dict={"NextEra": [6452, 7801]},
        years=[2020],
        output_dir_path=f"{os.getcwd()}",
    )
    # Make sure there is something there
    mapping_spreadsheet = pd.read_excel(
        f"{os.getcwd()}/NextEra_fix_FERC-EIA_overrides.xlsx"
    )
    if mapping_spreadsheet.empty:
        raise AssertionError("Mapping spreadsheet has no contents")

    # Remove this test file
    os.remove(f"{os.getcwd()}/NextEra_fix_FERC-EIA_overrides.xlsx")
