"""Validate post-ETL Boiler Fuel data from EIA 923."""

import logging

import pytest

import pudl

logger = logging.getLogger(__name__)


###############################################################################
# Tests validating data against physically reasonable boundary values:
###############################################################################

def test_coal_heat_content(pudl_out_orig, live_pudl_db):
    """Check that the distribution of coal heat content per unit is valid."""
    if not live_pudl_db:
        raise AssertionError("Data validation only works with a live PUDL DB.")

    for args in pudl.validate.bf_eia923_coal_heat_content:
        pudl.validate.vs_bounds(pudl_out_orig.bf_eia923(), **args)


def test_oil_heat_content(pudl_out_orig, live_pudl_db):
    """Check that the distribution of oil heat content per unit is valid."""
    if not live_pudl_db:
        raise AssertionError("Data validation only works with a live PUDL DB.")

    for args in pudl.validate.bf_eia923_oil_heat_content:
        pudl.validate.vs_bounds(pudl_out_orig.bf_eia923(), **args)


@pytest.mark.xfail
def test_gas_heat_content(pudl_out_orig, live_pudl_db):
    """
    Check that the distribution of gas heat content per unit is valid.

    Currently failing, due to a population of gas records with 1/10th the
    expected heat content. Reporting error? Unit conversion?

    See: https://github.com/catalyst-cooperative/pudl/issues/391

    """
    if not live_pudl_db:
        raise AssertionError("Data validation only works with a live PUDL DB.")

    for args in pudl.validate.bf_eia923_gas_heat_content:
        pudl.validate.vs_bounds(pudl_out_orig.bf_eia923(), **args)


def test_coal_ash_content(pudl_out_orig, live_pudl_db):
    """Check that distribution of coal ash content is valid."""
    if not live_pudl_db:
        raise AssertionError("Data validation only works with a live PUDL DB.")

    for args in pudl.validate.bf_eia923_coal_ash_content:
        pudl.validate.vs_bounds(pudl_out_orig.bf_eia923(), **args)


def test_coal_sulfur_content(pudl_out_orig, live_pudl_db):
    """Check that distribution of coal sulfur content is valid."""
    if not live_pudl_db:
        raise AssertionError("Data validation only works with a live PUDL DB.")

    for args in pudl.validate.bf_eia923_coal_sulfur_content:
        pudl.validate.vs_bounds(pudl_out_orig.bf_eia923(), **args)


###############################################################################
# Tests validating distributions against historical subsamples of themselves
###############################################################################


def test_self_vs_historical(pudl_out_orig, live_pudl_db):
    """Validate the whole dataset against historical annual subsamples."""
    if not live_pudl_db:
        raise AssertionError("Data validation only works with a live PUDL DB.")

    for args in pudl.validate.bf_eia923_self:
        pudl.validate.vs_self(pudl_out_orig.bf_eia923(), **args)


def test_agg_vs_historical(pudl_out_orig, pudl_out_eia, live_pudl_db):
    """Validate whole dataset against aggregated historical values."""
    if not live_pudl_db:
        raise AssertionError("Data validation only works with a live PUDL DB.")

    for args in pudl.validate.bf_eia923_agg:
        pudl.validate.vs_historical(pudl_out_orig.bf_eia923(),
                                    pudl_out_eia.bf_eia923(),
                                    **args)
