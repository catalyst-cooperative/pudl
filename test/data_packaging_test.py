"""Tests excercising data packaging for use with PyTest."""

import logging
import pytest
import pudl
from pudl.settings import SETTINGS

logger = logging.getLogger(__name__)


def test_data_packaging(ferc1_engine):
    """
    Generate limited packages for testing.
    """
    # TODO: we need to ensure that the ferc db is set before running this test.
    print('do the thing')
    settings_init = pudl.settings.settings_init(
        settings_file='settings_pudl_package_test.yml')
    out_dir = SETTINGS['out_dir']
    pudl.output.export.generate_data_packages(settings_init, out_dir)
