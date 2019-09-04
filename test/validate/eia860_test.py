"""Validate post-ETL EIA 860 data and the associated derived outputs."""

import logging

from scipy import stats

from pudl import helpers

logger = logging.getLogger(__name__)


def test_plants_eia860(pudl_out_eia):
    """Sanity checks for EIA 860 plants data."""
    logger.info('Reading EIA 860 plant data...')
    logger.info(f"{len(pudl_out_eia.plants_eia860())} records found.")


def test_utils_eia860(pudl_out_eia):
    """Sanity checks for EIA 860 utility data."""
    logger.info('Reading EIA 860 utility data...')
    logger.info(f"{len(pudl_out_eia.utils_eia860())} records found.")


def test_pu_eia860(pudl_out_eia):
    """Sanity checks for EIA 860 plant & utility data."""
    logger.info('Reading EIA 860 plant & utility data...')
    logger.info(f"{len(pudl_out_eia.pu_eia860())} records found.")


def test_gens_eia860(pudl_out_eia):
    """Sanity checks for EIA 860 generator data."""
    logger.info('Reading EIA 860 generator data...')
    logger.info(f"{len(pudl_out_eia.gens_eia860())} records found.")


def test_bga_eia860(pudl_out_eia):
    """Sanity checks for EIA 860 boiler-generator associations."""
    logger.info('Reading original EIA 860 boiler-generator associations...')
    logger.info(f"{len(pudl_out_eia.bga_eia860())} records found.")


def test_own_eia860(pudl_out_eia):
    """Sanity checks for EIA 860 generator ownership data."""
    logger.info('Reading EIA 860 generator ownership data...')
    own_out = pudl_out_eia.own_eia860()
    logger.info(f"{len(own_out)} records found.")

    # Verify that the reported ownership fractions add up to something very
    # close to 1.0 (i.e. that the full ownership of each generator is
    # specified by the EIA860 data)
    own_gb = own_out.groupby(['report_date', 'plant_id_eia', 'generator_id'])
    own_sum = own_gb['fraction_owned'].agg(helpers.sum_na).reset_index()
    logger.info(
        f"{len(own_sum[own_sum.fraction_owned.isnull()])} generator-years have no ownership data.")

    own_sum = own_sum.dropna()
    logger.info(
        f"{len(own_sum[own_sum.fraction_owned < 0.98])} generator-years have incomplete ownership data.")
    if not max(own_sum['fraction_owned'] < 1.02):
        raise AssertionError(
            "Plants with more than 100% ownership found..."
        )
    # There might be a few generators with incomplete ownership but virtually
    # everything should be pretty fully described. If not, let us know. The
    # 0.1 threshold means 0.1% -- i.e. less than 1 in 1000 is partial.
    if stats.percentileofscore(own_sum.fraction_owned, 0.98) >= 0.1:
        raise AssertionError(
            "Found too many generators with partial ownership data."
        )
