"""Validate post-ETL Generation Fuel data from EIA 923."""

import logging

import pytest

import pudl
import pudl.validate as pv

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


@pytest.mark.parametrize(
    "cases", [
        pytest.param(pv.gf_eia923_coal_heat_content, id="coal_heat_content"),
        pytest.param(pv.gf_eia923_oil_heat_content, id="oil_heat_content"),
    ]
)
def test_vs_bounds(pudl_out_orig, live_pudl_db, cases):
    """Verify that report fuel heat content per unit is reasonable."""
    if not live_pudl_db:
        raise AssertionError("Data validation only works with a live PUDL DB.")

    for args in cases:
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
