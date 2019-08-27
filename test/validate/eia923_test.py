"""Validate post-ETL EIA 923 data and associated derived outputs."""

import logging

import pytest

import pudl

logger = logging.getLogger(__name__)


@pytest.mark.eia923
@pytest.mark.post_etl
def test_frc_eia923(pudl_out_orig, pudl_out_eia, live_pudl_db):
    """Sanity checks for EIA 923 Fuel Recepts and Costs output."""
    if not live_pudl_db:
        raise AssertionError("Output tests only work with a live PUDL DB.")
    orig_df = pudl_out_orig.frc_eia923()
    test_df = pudl_out_eia.frc_eia923()

    for args in pudl.validate.agg_test_args:
        pudl.validate.vs_historical(orig_df, test_df, **args)

    for args in pudl.validate.abs_test_args:
        pudl.validate.vs_historical(orig_df, orig_df, **args)


@pytest.mark.eia923
@pytest.mark.post_etl
def test_gf_eia923(pudl_out_eia):
    """Sanity checks for EIA 923 Generator Fuel output."""
    logger.info("Reading EIA 923 Generator Fuel data...")
    logger.info(f"Successfully pulled{len(pudl_out_eia.gf_eia923())} records.")


@pytest.mark.eia923
@pytest.mark.post_etl
def test_bf_eia923(pudl_out_eia):
    """Sanity checks for EIA 923 Boiler Fuel output."""
    logger.info("Reading EIA 923 Boiler Fuel data...")
    logger.info(
        f"Successfully pulled {len(pudl_out_eia.bf_eia923())} records.")


@pytest.mark.eia923
@pytest.mark.post_etl
def test_gen_eia923(pudl_out_eia):
    """Sanity checks for EIA 923 Generation output."""
    logger.info("Reading EIA 923 Generation data...")
    logger.info(
        f"Successfully pulled {len(pudl_out_eia.gen_eia923())} records.")
