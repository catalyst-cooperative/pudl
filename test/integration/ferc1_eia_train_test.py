"""Tests for the validation functions in the ferc1_eia_train module."""

import importlib.resources
import logging

import pandas as pd
import pytest

from pudl.analysis.ferc1_eia import restrict_train_connections_on_date_range
from pudl.analysis.ferc1_eia_train import validate_override_fixes
from pudl.output.pudltabl import PudlTabl

logger = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def pudl_out(pudl_engine, pudl_datastore_fixture):
    """A PUDL output object for use in CI."""
    return PudlTabl(
        pudl_engine,
        ds=pudl_datastore_fixture,
        freq="AS",
        fill_tech_desc=False,
        fill_net_gen=False,
    )


@pytest.fixture(scope="module")
def utils_eia860(pudl_out):
    """The utils_eia860 output table."""
    return pudl_out.utils_eia860()


@pytest.fixture(scope="module")
def plant_part_list(pudl_out):
    """The plant_parts_eia output table."""
    return pudl_out.plant_parts_eia().reset_index()


@pytest.fixture(scope="module")
def ferc1_eia(pudl_out):
    """The ferc1_eia output table."""
    return pudl_out.ferc1_eia()


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
            [2020],
            ["4270_2019_plant_total_11241"],
            ["f1_steam_2020_12_454_0_3"],
            marks=pytest.mark.xfail(
                reason="EIA record year doesn't match FERC record year"
            ),
        ),
        pytest.param(
            ["True", "True"],
            [2020, 2020],
            ["4270_2020_plant_total_11241", "4270_2020_plant_total_11241"],
            ["f1_steam_2020_12_454_0_3", "f1_steam_2020_12_159_0_3"],
            marks=pytest.mark.xfail(reason="Duplicate EIA ids in training data"),
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
    pytest.set_trace()
    validate_override_fixes(
        validated_connections=test_df,
        utils_eia860=utils_eia860,
        ppl=plant_part_list,
        ferc1_eia=ferc1_eia,
        training_data=ferc1_eia_training_data_restricted,
        expect_override_overrides=True,
        allow_mismatched_utilities=True,
    )
