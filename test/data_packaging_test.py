"""Tests excercising data packaging for use with PyTest."""

import logging

import pytest

import pudl

logger = logging.getLogger(__name__)


@pytest.mark.data_package
def test_data_packaging(datastore_fixture, ferc1_engine,
                        pudl_settings_fixture, data_scope):
    """Generate limited packages for testing."""
    pudl.output.export.generate_data_packages(data_scope['pkg_bundle_settings'],
                                              pudl_settings_fixture)


@pytest.mark.data_package
def test_data_packaging_to_sqlite(pudl_settings_fixture):
    """Try flattening the data packages."""
    pudl.output.export.flatten_pudl_datapackages(pudl_settings_fixture,
                                                 pkg_bundle_dir_name=None,
                                                 pkg_name='pudl-all')

    pudl.output.export.pkg_to_sqlite_db(pudl_settings_fixture)
