"""PyTest cases related to the integration between FERC1 & EIA 860/923."""
import logging

import pytest

import pudl

logger = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def fast_out(pudl_engine):
    """A PUDL output object for use with Travis CI."""
    return pudl.output.pudltabl.PudlTabl(pudl_engine, freq="MS", fill=True, roll=True)


def test_fuel_ferc1(fast_out):
    """Pull FERC 1 Fuel Data."""
    logger.info("Pulling a year's worth of FERC1 Fuel data.")
    fuel_df = fast_out.fuel_ferc1()
    logger.info(f"Pulled {len(fuel_df)} Fuel FERC1 records.")


def test_plants_steam_ferc1(fast_out):
    """Pull FERC 1 Steam Plants."""
    logger.info("Pulling FERC1 Steam Plants")
    steam_df = fast_out.plants_steam_ferc1()
    logger.info(f"Pulled{len(steam_df)} FERC1 steam plants records.")


def test_fbp_ferc1(fast_out):
    """Calculate fuel consumption by plant for FERC 1 for one year of data."""
    logger.info("Calculating FERC1 Fuel by Plant.")
    fbp_df = fast_out.fbp_ferc1()
    logger.info(f"Generated {len(fbp_df)} FERC1 fuel by plant records.")


def test_bga_eia860(fast_out):
    """Pull original EIA 860 Boiler Generator Associations."""
    logger.info("Pulling the EIA 860 Boiler Generator Associations.")
    bga_df = fast_out.bga_eia860()
    logger.info(f"Generated {len(bga_df)} BGA EIA 860 records.")


def test_own_eia860(fast_out):
    """Read EIA 860 generator ownership data."""
    logger.info("Pulling the EIA 860 ownership data.")
    own_df = fast_out.own_eia860()
    logger.info(f"Generated {len(own_df)} EIA 860 ownership records.")


def test_gf_eia923(fast_out):
    """Read EIA 923 generator fuel data. (not used in MCOE)."""
    logger.info("Pulling the EIA 923 generator fuel data.")
    gf_df = fast_out.gf_eia923()
    logger.info(f"Generated {len(gf_df)} EIA 923 generator fuel records.")


def test_mcoe(fast_out):
    """Calculate MCOE."""
    logger.info("Calculating MCOE.")
    mcoe_df = fast_out.mcoe()
    logger.info(f"Generated {len(mcoe_df)} MCOE records.")
