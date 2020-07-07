"""
Validate post-ETL FERC Form 1 data and the associated derived outputs.

These tests depend on a FERC Form 1 specific PudlTabl output object, which is
a parameterized fixture that has session scope.
"""
import logging

import numpy as np
import pytest

from pudl import validate as pv

logger = logging.getLogger(__name__)


def test_fbp_ferc1_missing_fractions(pudl_out_ferc1, live_pudl_db):
    """Check whether FERC 1 fuel costs by plant appear to be complete."""
    if not live_pudl_db:
        raise AssertionError("Data validation only works with a live PUDL DB.")
    fbp_ferc1 = pudl_out_ferc1.fbp_ferc1()

    # Make sure we're not missing any costs...
    if not np.isclose(fbp_ferc1
                      .filter(regex=".*fraction_cost$")
                      .dropna(how="all")
                      .sum(axis="columns"), 1.0).all():
        raise ValueError("Fuel cost fractions do not sum to 1.0")

    # Make sure we're not missing any heat content...
    if not np.isclose(fbp_ferc1
                      .filter(regex=".*fraction_mmbtu$")
                      .dropna(how="all")
                      .sum(axis="columns"), 1.0).all():
        raise ValueError("Fuel heat content fractions do not sum to 1.0")


def test_fbp_ferc1_mismatched_fuels(pudl_out_ferc1, live_pudl_db):
    """Check whether FERC 1 primary fuel by cost and by heat content match."""
    if not live_pudl_db:
        raise AssertionError("Data validation only works with a live PUDL DB.")
    fbp_ferc1 = pudl_out_ferc1.fbp_ferc1()
    # High proportion of primary fuel by cost and by mmbtu should be the same
    mismatched_fuels = len(fbp_ferc1[
        fbp_ferc1.primary_fuel_by_cost != fbp_ferc1.primary_fuel_by_mmbtu
    ]) / len(fbp_ferc1)
    logger.info(f"{mismatched_fuels:.2%} of records "
                f"have mismatched primary fuel types.")
    if mismatched_fuels > 0.05:
        raise AssertionError(
            f"Too many records ({mismatched_fuels:.2%}) have mismatched "
            f"primary fuel types.")


def test_fbp_ferc1_mmbtu_cost_correlation(pudl_out_ferc1, live_pudl_db):
    """Check that the fuel cost fraction and mmbtu fractions are similar."""
    if not live_pudl_db:
        raise AssertionError("Data validation only works with a live PUDL DB.")
    fbp_ferc1 = pudl_out_ferc1.fbp_ferc1()
    for fuel in ["gas", "oil", "coal", "nuclear", "other"]:
        fuel_cols = [f"{fuel}_fraction_mmbtu", f"{fuel}_fraction_cost"]
        fuel_corr = fbp_ferc1[fuel_cols].corr().iloc[0, 1]
        if fuel_corr < 0.9:
            raise ValueError(
                f"{fuel} cost v mmbtu corrcoef is below 0.9: {fuel_corr}")


@pytest.mark.parametrize(
    "cases", [
        pytest.param(pv.fbp_ferc1_gas_cost_per_mmbtu_bounds,
                     id="gas_cost_per_mmbtu"),
        pytest.param(pv.fbp_ferc1_oil_cost_per_mmbtu_bounds,
                     id="oil_cost_per_mmbtu"),
        pytest.param(pv.fbp_ferc1_coal_cost_per_mmbtu_bounds,
                     id="coal_cost_per_mmbtu"),
    ]
)
def test_vs_bounds(pudl_out_ferc1, live_pudl_db, cases):
    """Test distributions of calculated fuel by plant values."""
    if not live_pudl_db:
        raise AssertionError("Data validation only works with a live PUDL DB.")

    fbp_ferc1 = pudl_out_ferc1.fbp_ferc1()
    for f in ["gas", "oil", "coal", "waste", "nuclear", "other"]:
        fbp_ferc1[f"{f}_cost_per_mmbtu"] = (
            (fbp_ferc1[f"{f}_fraction_cost"] * fbp_ferc1["fuel_cost"]) /
            (fbp_ferc1[f"{f}_fraction_mmbtu"] * fbp_ferc1["fuel_mmbtu"])
        )

    for case in cases:
        pv.vs_bounds(fbp_ferc1, **case)


def test_self_vs_historical(pudl_out_ferc1, live_pudl_db):
    """Validate fuel by plants vs. historical data."""
    if not live_pudl_db:
        raise AssertionError("Data validation only works with a live PUDL DB.")
    for args in pv.fbp_ferc1_self:
        pv.vs_self(pudl_out_ferc1.fbp_ferc1(), **args)
