"""Validate post-ETL Generation Fuel data from EIA 923."""

import logging

import pytest

import pudl
from pudl import validate as pv

logger = logging.getLogger(__name__)


@pytest.mark.parametrize(
    "cases",
    [
        pytest.param(pv.gf_eia923_coal_heat_content, id="coal_heat_content"),
        pytest.param(pv.gf_eia923_oil_heat_content, id="oil_heat_content"),
    ],
)
def test_vs_bounds(pudl_out_eia, live_dbs, cases):
    """Verify that report fuel heat content per unit is reasonable."""
    if not live_dbs:
        pytest.skip("Data validation only works with a live PUDL DB.")
    # This test should only run on the un-aggregated data:
    if pudl_out_eia.freq is not None:
        pytest.skip("Test should only run on un-aggregated data.")

    for args in cases:
        pudl.validate.vs_bounds(pudl_out_eia.gf_eia923(), **args)
