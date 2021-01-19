"""Test incomplete / interim ETL functions during development."""

import logging

import pudl

logger = logging.getLogger(__name__)


def test_interim_get_census2010_gdf(pudl_settings_fixture, pudl_datastore_fixture):
    """Make sure that service_territory.get_census."""
    logger.info("Running pudl.analysis.service_territory.get_census2010_gdf.")
    pudl.analysis.service_territory.get_census2010_gdf(
        pudl_settings_fixture, "state", pudl_datastore_fixture)
    pudl.analysis.service_territory.get_census2010_gdf(
        pudl_settings_fixture, "county", pudl_datastore_fixture)
    pudl.analysis.service_territory.get_census2010_gdf(
        pudl_settings_fixture, "tract", pudl_datastore_fixture)
