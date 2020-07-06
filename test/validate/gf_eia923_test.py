"""Validate post-ETL Generation Fuel data from EIA 923."""
import logging

import pytest

import pudl
from pudl import validate as pv

logger = logging.getLogger(__name__)


def test_fuel_for_electricity(pudl_out_eia, live_pudl_db):
    """Ensure fuel used for electricity is less than or equal to all fuel."""
    if not live_pudl_db:
        raise AssertionError("Data validation only works with a live PUDL DB.")

    gf_eia923 = pudl_out_eia.gf_eia923()

    excess_fuel = \
        gf_eia923.fuel_consumed_for_electricity_mmbtu > gf_eia923.fuel_consumed_mmbtu

    if excess_fuel.any():
        raise ValueError(
            "Fuel consumed for electricity is greater than all fuel consumed!"
        )


@pytest.mark.parametrize(
    "cases", [
        pytest.param(pv.gf_eia923_coal_heat_content, id="coal_heat_content"),
        pytest.param(pv.gf_eia923_oil_heat_content, id="oil_heat_content"),
    ]
)
def test_vs_bounds(pudl_out_eia, live_pudl_db, cases):
    """Verify that report fuel heat content per unit is reasonable."""
    if not live_pudl_db:
        raise AssertionError("Data validation only works with a live PUDL DB.")
    # This test should only run on the un-aggregated data:
    if pudl_out_eia.freq is not None:
        pytest.skip("Test should only run on un-aggregated data.")

    for args in cases:
        pudl.validate.vs_bounds(pudl_out_eia.gf_eia923(), **args)


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
    if pudl_out_eia.freq is None:
        pytest.skip("Only run if pudl_out_eia != pudl_out_orig.")

    for args in pudl.validate.gf_eia923_agg:
        pudl.validate.vs_historical(pudl_out_orig.gf_eia923(),
                                    pudl_out_eia.gf_eia923(),
                                    **args)
