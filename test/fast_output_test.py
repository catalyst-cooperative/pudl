"""PyTest cases related to the integration between FERC1 & EIA 860/923."""
import logging

import pytest

import pudl

logger = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def fast_out(pudl_engine):
    """A PUDL output object for use with Travis CI."""
    return pudl.output.pudltabl.PudlTabl(pudl_engine, freq="MS")


def test_mcoe(fast_out):
    """Calculate MCOE for a single year of data..."""
    logger.info("Calculating MCOE for a single year of data.")
    mcoe_df = fast_out.mcoe()
    logger.info(f"Generated {len(mcoe_df)} MCOE records.")


def test_fbp_ferc1(fast_out):
    """Calculate fuel consumption by plant for FERC 1 for one year of data."""
    logger.info("Calculating FERC1 Fuel by Plant for a single year of data.")
    fbp_df = fast_out.fbp_ferc1()
    logger.info(f"Generated {len(fbp_df)} FERC1 fuel by plant records.")
