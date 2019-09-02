
"""Validate post-ETL Fuel Receipts and Costs data from EIA 923."""

import logging

import pytest

import pudl

logger = logging.getLogger(__name__)


###############################################################################
# Tests validating data against physically reasonable boundary values:
###############################################################################
@pytest.mark.xfail
def test_coal_mercury_content_ppm(pudl_out_orig, live_pudl_db):
    """
    Validate reported coal mercury content is consistent with USGS data.

    Based on USGS FS095-01: https://pubs.usgs.gov/fs/fs095-01/fs095-01.html
    """
    if not live_pudl_db:
        raise AssertionError("Data validation only works with a live PUDL DB.")

    for args in pudl.validate.frc_eia923_coal_mercury_content:
        pudl.validate.vs_bounds(pudl_out_orig.frc_eia923(), **args)


def test_coal_heat_content(pudl_out_orig, live_pudl_db):
    """
    Validate reported coal heat content is consistent with IEA definitions.

    Checks that median value of heat content per unit delivered is within
    specified bounds for each of bituminous, sub-bituminous, and lignite, as
    well as all coal.

    Based on: https://www.iea.org/statistics/resources/balancedefinitions/
    """
    if not live_pudl_db:
        raise AssertionError("Data validation only works with a live PUDL DB.")

    for args in pudl.validate.frc_eia923_coal_heat_content:
        pudl.validate.vs_bounds(pudl_out_orig.frc_eia923(), **args)


def test_coal_ash_content(pudl_out_orig, live_pudl_db):
    """
    Validate reported coal ash content is realistic.

    Based on historical ranges of ash content reported in EIA 923 for
    bituminous, sub-bituminous, lignite, and coal overall.
    """
    if not live_pudl_db:
        raise AssertionError("Data validation only works with a live PUDL DB.")

    for args in pudl.validate.frc_eia923_coal_ash_content:
        pudl.validate.vs_bounds(pudl_out_orig.frc_eia923(), **args)


def test_coal_moisture_content(pudl_out_orig, live_pudl_db):
    """
    Validate that reported coal moisture content is realistic.

    Based on historical ranges of moisture content reported in EIA 923 for
    bituminous, sub-bituminous, lignite, and coal overall.
    """
    if not live_pudl_db:
        raise AssertionError("Data validation only works with a live PUDL DB.")

    for args in pudl.validate.frc_eia923_coal_heat_content:
        pudl.validate.vs_bounds(pudl_out_orig.frc_eia923(), **args)


def test_coal_sulfur_content(pudl_out_orig, live_pudl_db):
    """Validate that reported coal sulfur content is realistic."""
    if not live_pudl_db:
        raise AssertionError("Data validation only works with a live PUDL DB.")

    for args in pudl.validate.frc_eia923_coal_sulfur_content:
        pudl.validate.vs_bounds(pudl_out_orig.frc_eia923(), **args)


def test_oil_heat_content(pudl_out_orig, live_pudl_db):
    """Validate that reported oil heat content is realistic.

    Needs to test oil overall, and individually DFO + KER?
    """
    if not live_pudl_db:
        raise AssertionError("Data validation only works with a live PUDL DB.")

    for args in pudl.validate.frc_eia923_oil_heat_content:
        pudl.validate.vs_bounds(pudl_out_orig.frc_eia923(), **args)


@pytest.mark.xfail
def test_gas_heat_content(pudl_out_orig, live_pudl_db):
    """Validate that reported gas heat content is realistic.

    Fails because there's a weird little 0.1x population.
    """
    if not live_pudl_db:
        raise AssertionError("Data validation only works with a live PUDL DB.")

    for args in pudl.validate.frc_eia923_gas_heat_content:
        pudl.validate.vs_bounds(pudl_out_orig.frc_eia923(), **args)

###############################################################################
# Tests validating distributions against historical subsamples of themselves
###############################################################################


def test_self_vs_historical(pudl_out_orig, live_pudl_db):
    """Validate the whole dataset against historical annual subsamples."""
    if not live_pudl_db:
        raise AssertionError("Data validation only works with a live PUDL DB.")

    for args in pudl.validate.frc_eia923_self:
        pudl.validate.vs_self(pudl_out_orig.frc_eia923(), **args)


def test_agg_vs_historical(pudl_out_orig, pudl_out_eia, live_pudl_db):
    """Validate whole dataset against aggregated historical values."""
    if not live_pudl_db:
        raise AssertionError("Data validation only works with a live PUDL DB.")

    for args in pudl.validate.frc_eia923_agg:
        pudl.validate.vs_historical(pudl_out_orig.frc_eia923(),
                                    pudl_out_eia.frc_eia923(),
                                    **args)
