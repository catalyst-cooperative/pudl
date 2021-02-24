"""Validate post-ETL Generators data from EIA 860."""
import logging

import pytest

import pudl

logger = logging.getLogger(__name__)


###############################################################################
# Tests validating data against physically reasonable boundary values:
###############################################################################


def test_capacity_bounds(pudl_out_eia, live_pudl_db):
    """Check that the distribution of coal heat content per unit is valid."""
    if not live_pudl_db:
        raise AssertionError("Data validation only works with a live PUDL DB.")
    if pudl_out_eia.freq is not None:
        pytest.skip("Test should only run on un-aggregated data.")

    for args in pudl.validate.gens_eia860_vs_bound:
        pudl.validate.vs_bounds(pudl_out_eia.gens_eia860(), **args)


def test_capacity_self(pudl_out_eia, live_pudl_db):
    """Check that the distribution of coal heat content per unit is valid."""
    if not live_pudl_db:
        raise AssertionError("Data validation only works with a live PUDL DB.")
    if pudl_out_eia.freq is not None:
        pytest.skip("Test should only run on un-aggregated data.")

    for args in pudl.validate.gens_eia860_self:
        pudl.validate.vs_self(pudl_out_eia.gens_eia860(), **args)
