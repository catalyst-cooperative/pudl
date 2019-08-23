"""Tests excercising data packaging for use with PyTest."""

import logging

import pytest

import pudl

logger = logging.getLogger(__name__)


@pytest.mark.data_package
def test_data_packaging(datastore_fixture, ferc1_engine,
                        pudl_settings_fixture, data_scope):
    """Generate limited packages for testing."""
    pkg_bundle_dir_name = 'pudl-test'
    pudl.etl_pkg.generate_data_packages(
        data_scope['pkg_bundle_settings'],
        pudl_settings_fixture,
        pkg_bundle_dir_name=pkg_bundle_dir_name)


@pytest.mark.data_package
def test_data_packaging_to_sqlite(pudl_settings_fixture):
    """Try flattening the data packages."""
    pkg_bundle_dir_name = 'pudl-test'
    pudl.convert.flatten_datapkgs.flatten_pudl_datapackages(
        pudl_settings_fixture,
        pkg_bundle_dir_name=pkg_bundle_dir_name,
        pkg_name='pudl-all')

    pudl.convert.datapkg_to_sqlite.pkg_to_sqlite_db(
        pudl_settings_fixture,
        pkg_bundle_dir_name=pkg_bundle_dir_name,
        pkg_name='pudl-all')
