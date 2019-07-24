"""Tests excercising data packaging for use with PyTest."""

import logging
import pathlib

import pudl

logger = logging.getLogger(__name__)


def test_data_packaging(ferc1_engine, pudl_settings_fixture):
    """Generate limited packages for testing."""
    # TODO: we need to ensure that the ferc db is set before running this test.
    settings_file = pathlib.Path(pudl_settings_fixture['settings_dir'],
                                 'settings_datapackage_default.yml')
    pkg_bundle_settings = pudl.settings.grab_package_settings(
        pudl_settings_fixture, settings_file)
    pudl.output.export.generate_data_packages(pkg_bundle_settings,
                                              pudl_settings_fixture)
