"""Validate post-ETL Generation Fuel data from EIA 923."""

import logging

import pytest

import pudl

logger = logging.getLogger(__name__)


def test_fuel_for_electricity(pudl_out_orig, pudl_out_eia, live_pudl_db):
    """Ensure fuel used for electricity is less than or equal to all fuel."""
    if not live_pudl_db:
        raise AssertionError("Data validation only works with a live PUDL DB.")

    gf_eia923_orig = pudl_out_orig.gf_eia923()
    gf_eia923_agg = pudl_out_eia.gf_eia923()

    excess_fuel_orig = \
        gf_eia923_orig.fuel_consumed_for_electricity_mmbtu > gf_eia923_orig.fuel_consumed_mmbtu

    excess_fuel_agg = \
        gf_eia923_agg.fuel_consumed_for_electricity_mmbtu > gf_eia923_agg.fuel_consumed_mmbtu

    if excess_fuel_orig.any():
        raise ValueError(
            f"Fuel consumed for electricity is greater than all fuel consumed!"
        )
    if excess_fuel_agg.any():
        raise ValueError(
            f"Fuel consumed for electricity is greater than all fuel consumed!"
        )


def test_coal_heat_content(pudl_out_orig, live_pudl_db):
    """Check that the distribution of coal heat content per unit is valid."""
    if not live_pudl_db:
        raise AssertionError("Data validation only works with a live PUDL DB.")

    for args in pudl.validate.gf_eia923_coal_heat_content:
        pudl.validate.vs_bounds(pudl_out_orig.gf_eia923(), **args)


def test_oil_heat_content(pudl_out_orig, live_pudl_db):
    """Check that the distribution of oil heat content per unit is valid."""
    if not live_pudl_db:
        raise AssertionError("Data validation only works with a live PUDL DB.")

    for args in pudl.validate.gf_eia923_oil_heat_content:
        pudl.validate.vs_bounds(pudl_out_orig.gf_eia923(), **args)


@pytest.mark.xfail
def test_gas_heat_content(pudl_out_orig, live_pudl_db):
    """
    Check that the distribution of gas heat content per unit is valid.

    Currently failing, due to populations of gas records with 0.1x, 0.25x and
    0.5x the expected heat content. Reporting error? Unit conversion?

    See: https://github.com/catalyst-cooperative/pudl/issues/391

    """
    if not live_pudl_db:
        raise AssertionError("Data validation only works with a live PUDL DB.")

    for args in pudl.validate.gf_eia923_gas_heat_content:
        pudl.validate.vs_bounds(pudl_out_orig.gf_eia923(), **args)


###############################################################################
# Tests validating distributions against historical subsamples of themselves
# Note that all of the fields we're testing in this table are the
# fuel_type_code_pudl fields which are simplified lumpings of the other fuel
# types, and aren't as useful to test against their historical selves. So we're
# only testing the aggregation (i.e. there's no test_self_vs_historical() here)
################################################################################


def test_agg_vs_historical(pudl_out_orig, pudl_out_eia, live_pudl_db):
    """Validate whole dataset against aggregated historical values."""
    if not live_pudl_db:
        raise AssertionError("Data validation only works with a live PUDL DB.")

    for args in pudl.validate.bf_eia923_agg:
        pudl.validate.vs_historical(pudl_out_orig.bf_eia923(),
                                    pudl_out_eia.bf_eia923(),
                                    **args)
