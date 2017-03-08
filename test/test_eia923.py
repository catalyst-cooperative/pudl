"""Tests exercising the eia923 module for use with PyTest."""

import pytest
import pudl.eia923
import pudl.constants as pc


def test_get_pages_eia923():
    """Pull in all pages of EIA923 that we expect to work."""
    eia923_dfs = {}
    for page in pc.pagemap_eia923.index:
        eia923_dfs[page] = \
            pudl.eia923.get_eia923_page(page,
                                        years=pc.eia923_working_years,
                                        verbose=True)
