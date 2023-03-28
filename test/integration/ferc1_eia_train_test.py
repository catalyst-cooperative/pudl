"""Tests for the validation functions in the ferc1_eia_train module."""

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
def plant_part_list(fast_out_annual):
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
        importlib.resources.path("pudl.package_data.glue", "ferc1_eia_train.csv")
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
        # This param fails for the wrong reason -- because the 2019 data isn't available.
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
    ],
)
def test_validate_override_fixes(
    utils_eia860,
    plant_part_list,
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
        ppl=plant_part_list,
        ferc1_eia=ferc1_eia,
        training_data=ferc1_eia_training_data_restricted,
        expect_override_overrides=True,
        allow_mismatched_utilities=True,
    )


def test_generate_all_override_spreadsheets(plant_part_list, ferc1_eia, utils_eia860):
    """Test the genation of the override spreadsheet for mapping FERC-EIA records."""
    # Create the test spreadsheet
    generate_all_override_spreadsheets(
        ferc1_eia,
        plant_part_list,
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
