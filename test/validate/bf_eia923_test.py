"""Validate post-ETL Boiler Fuel data from EIA 923."""
import logging

import pytest

import pudl
from pudl import validate as pv

logger = logging.getLogger(__name__)


###############################################################################
# Tests validating data against physically reasonable boundary values:
###############################################################################

@pytest.mark.parametrize(
    "cases", [
        pytest.param(pv.bf_eia923_coal_heat_content, id="coal_heat_content"),
        pytest.param(pv.bf_eia923_oil_heat_content, id="oil_heat_content"),
        pytest.param(
            pv.bf_eia923_gas_heat_content,
            id="gas_heat_content",
            marks=pytest.mark.xfail(reason="EIA 923 Reporting Errors?")
        ),
        pytest.param(pv.bf_eia923_coal_ash_content, id="coal_ash_content"),
        pytest.param(
            pv.bf_eia923_coal_sulfur_content, id="coal_sulfur_content")
    ]
)
def test_vs_bounds(pudl_out_eia, live_pudl_db, cases):
    """Validate data reported in bf_eia923 against static boundaries."""
    if not live_pudl_db:
        raise AssertionError("Data validation only works with a live PUDL DB.")
    if pudl_out_eia.freq is not None:
        pytest.skip("Test only runs on un-aggregated data.")

    for args in cases:
        pudl.validate.vs_bounds(pudl_out_eia.bf_eia923(), **args)


###############################################################################
# Tests validating distributions against historical subsamples of themselves
###############################################################################


def test_self_vs_historical(pudl_out_eia, live_pudl_db):
    """Validate the whole dataset against historical annual subsamples."""
    if not live_pudl_db:
        raise AssertionError("Data validation only works with a live PUDL DB.")
    if pudl_out_eia.freq is not None:
        pytest.skip("Test only runs on un-aggregated data.")

    for args in pudl.validate.bf_eia923_self:
        pudl.validate.vs_self(pudl_out_eia.bf_eia923(), **args)


def test_agg_vs_historical(pudl_out_orig, pudl_out_eia, live_pudl_db):
    """Validate whole dataset against aggregated historical values."""
    if not live_pudl_db:
        raise AssertionError("Data validation only works with a live PUDL DB.")
    if pudl_out_eia.freq is None:
        pytest.skip("Only run if pudl_out_eia != pudl_out_orig.")

    for args in pudl.validate.bf_eia923_agg:
        pudl.validate.vs_historical(pudl_out_orig.bf_eia923(),
                                    pudl_out_eia.bf_eia923(),
                                    **args)
