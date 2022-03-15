"""Validate post-ETL Generation data from EIA 923."""

import logging

import pytest

logger = logging.getLogger(__name__)


def test_gen_eia923(pudl_out_eia, live_dbs):
    """Sanity checks for EIA 923 Generation output."""
    if not live_dbs:
        pytest.skip("Data validation only works with a live PUDL DB.")
    logger.info("Reading EIA 923 Generation data...")
    logger.info(f"Successfully pulled {len(pudl_out_eia.gen_eia923())} records.")
