"""Tests excercising data packaging for use with PyTest."""

import logging

import pytest

logger = logging.getLogger(__name__)


@pytest.mark.data_package
def test_data_packaging(data_packaging):
    """Generate limited packages for testing."""
    pass


@pytest.mark.data_package
def test_data_packaging_to_sqlite(data_packaging_to_sqlite):
    """Try flattening the data packages."""
    pass
