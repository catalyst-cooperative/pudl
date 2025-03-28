"""Validate post-ETL FERC Form 1 data and the associated derived outputs.

These tests depend on a FERC Form 1 specific PudlTabl output object, which is a
parameterized fixture that has session scope.
"""

import logging

import pytest

from pudl import validate as pv

logger = logging.getLogger(__name__)


@pytest.mark.parametrize(
    "cases",
    [
        pytest.param(pv.fbp_ferc1_gas_cost_per_mmbtu_bounds, id="gas_cost_per_mmbtu"),
        pytest.param(pv.fbp_ferc1_oil_cost_per_mmbtu_bounds, id="oil_cost_per_mmbtu"),
        pytest.param(pv.fbp_ferc1_coal_cost_per_mmbtu_bounds, id="coal_cost_per_mmbtu"),
    ],
)
def test_vs_bounds(pudl_out_ferc1, live_dbs, cases):
    """Test distributions of calculated fuel by plant values."""
    if not live_dbs:
        pytest.skip("Data validation only works with a live PUDL DB.")

    fbp_ferc1 = pudl_out_ferc1.fbp_ferc1()
    for f in ["gas", "oil", "coal", "waste", "nuclear"]:
        fbp_ferc1[f"{f}_cost_per_mmbtu"] = (
            fbp_ferc1[f"{f}_fraction_cost"] * fbp_ferc1["fuel_cost"]
        ) / (fbp_ferc1[f"{f}_fraction_mmbtu"] * fbp_ferc1["fuel_mmbtu"])

    for case in cases:
        pv.vs_bounds(fbp_ferc1, **case)


def test_self_vs_historical(pudl_out_ferc1, live_dbs):
    """Validate fuel by plants vs.

    historical data.
    """
    if not live_dbs:
        pytest.skip("Data validation only works with a live PUDL DB.")
    else:
        fbp_ferc1 = pudl_out_ferc1.fbp_ferc1()
        for f in ["gas", "oil", "coal", "waste", "nuclear"]:
            fbp_ferc1[f"{f}_cost_per_mmbtu"] = (
                fbp_ferc1[f"{f}_fraction_cost"] * fbp_ferc1["fuel_cost"]
            ) / (fbp_ferc1[f"{f}_fraction_mmbtu"] * fbp_ferc1["fuel_mmbtu"])
        for args in pv.fbp_ferc1_self:
            pv.vs_self(fbp_ferc1, **args)
