"""Tests for the validation functions in the ferc1_eia_train module."""

import importlib.resources
import logging
from io import StringIO

import pandas as pd
import pytest

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
    "verified,record_id_eia_override_1,record_id_ferc1",
    [pytest.param("True", "10773_2005_plant_total_19876", "f1_steam_2005_12_186_0_1")],
)
def test_validate_override_fixes(
    utils_eia860,
    plant_part_list,
    ferc1_eia,
    ferc1_eia_training_data,
    verified,
    record_id_eia_override_1,
    record_id_ferc1,
):
    """Test."""
    test_df = pd.read_csv(
        StringIO(
            f"""verified,record_id_eia_override_1,record_id_ferc1
    {verified},{record_id_eia_override_1},{record_id_ferc1}
    """
        ),
    ).assign(verified=lambda x: x.verified.astype("bool"))
    # pytest.set_trace()
    validate_override_fixes(
        validated_connections=test_df,
        utils_eia860=utils_eia860,
        ppl=plant_part_list,
        ferc1_eia=ferc1_eia,
        training_data=ferc1_eia_training_data,
        expect_override_overrides=True,
        allow_mismatched_utilities=True,
    )
