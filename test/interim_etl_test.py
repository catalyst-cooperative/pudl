"""Test incomplete / interim ETL functions during development."""

import logging

import pytest

import pudl

logger = logging.getLogger(__name__)


def test_interim_eia861_etl(pudl_settings_fixture):
    """Make sure that the EIA 861 Extract-Transform steps work."""
    logger.info("Running the interim EIA 861 ETL process! (~2 minutes)")
    _ = pudl.transform.eia861.transform(
        pudl.extract.eia861.Extractor()
        .extract(pudl.constants.working_years["eia861"], testing=True)
    )


def test_interim_ferc714_etl(pudl_settings_fixture, fast_tests):
    """Make sure that the EIA 861 Extract-Transform steps work."""
    if fast_tests:
        pytest.skip()
    logger.info("Running the interim FERC 714 ETL process! (~11 minutes)")
    _ = pudl.transform.ferc714.transform(pudl.extract.ferc714.extract())
