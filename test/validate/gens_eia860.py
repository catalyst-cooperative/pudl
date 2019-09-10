"""Validate post-ETL Generators data from EIA 860."""

import logging

import pudl

logger = logging.getLogger(__name__)


###############################################################################
# Tests validating data against physically reasonable boundary values:
###############################################################################


def test_capacity_bounds(pudl_out_orig, live_pudl_db):
    """Check that the distribution of coal heat content per unit is valid."""
    if not live_pudl_db:
        raise AssertionError("Data validation only works with a live PUDL DB.")

    for args in pudl.validate.gens_eia860_vs_bound:
        pudl.validate.vs_bounds(pudl_out_orig.gens_eia860(), **args)


def test_capacity_self(pudl_out_orig, live_pudl_db):
    """Check that the distribution of coal heat content per unit is valid."""
    if not live_pudl_db:
        raise AssertionError("Data validation only works with a live PUDL DB.")

    for args in pudl.validate.gens_eia860_self:
        pudl.validate.vs_self(pudl_out_orig.gens_eia860(), **args)
