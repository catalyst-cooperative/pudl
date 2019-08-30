"""Validate post-ETL Boiler Fuel data from EIA 923."""

import logging

import pytest

logger = logging.getLogger(__name__)


@pytest.mark.eia923
@pytest.mark.post_etl
def test_bf_eia923(pudl_out_eia):
    """Sanity checks for EIA 923 Boiler Fuel output."""
    logger.info("Reading EIA 923 Boiler Fuel data...")
    logger.info(
        f"Successfully pulled {len(pudl_out_eia.bf_eia923())} records.")
