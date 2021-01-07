"""Test incomplete / interim ETL functions during development."""

import logging

import pudl

logger = logging.getLogger(__name__)


def test_interim_eia861_etl(pudl_settings_fixture, pudl_datastore_fixture):
    """Make sure that the EIA 861 Extract-Transform steps work."""
    logger.info("Running the interim EIA 861 ETL process! (~2 minutes)")
    _ = pudl.transform.eia861.transform(
        pudl.extract.eia861.Extractor(pudl_datastore_fixture)
        .extract(year=pudl.constants.working_partitions['eia861']['years'])
    )


def test_interim_ferc714_etl(pudl_settings_fixture):
    """Make sure that the EIA 861 Extract-Transform steps work."""
    logger.info("Running the interim FERC 714 ETL process!")
    _ = pudl.transform.ferc714.transform(
        pudl.extract.ferc714.extract(pudl_settings=pudl_settings_fixture))


def test_get_ferc714(pudl_settings_fixture):
    """Make sure we get a real file from the datastore."""
    fn = str(pudl.extract.ferc714.get_ferc714(pudl_settings_fixture))
    assert pudl_settings_fixture["pudl_in"] in fn
    assert fn[-11:] == "ferc714.zip"


def test_interim_get_census2010_gdf(pudl_settings_fixture):
    """Make sure that service_territory.get_census."""
    logger.info("Running pudl.analysis.service_territory.get_census2010_gdf.")
    pudl.analysis.service_territory.get_census2010_gdf(
        pudl_settings_fixture, "state")
    pudl.analysis.service_territory.get_census2010_gdf(
        pudl_settings_fixture, "county")
    pudl.analysis.service_territory.get_census2010_gdf(
        pudl_settings_fixture, "tract")
