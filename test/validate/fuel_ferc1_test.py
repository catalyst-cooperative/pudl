"""
Validate post-ETL FERC Form 1 data and the associated derived outputs.

These tests depend on a FERC Form 1 specific PudlTabl output object, which is
a parameterized fixture that has session scope.
"""
import logging

import pytest

from pudl import validate as pv

logger = logging.getLogger(__name__)


def test_fuel_ferc1_trivial(pudl_out_ferc1):
    """Test output routines for tables from FERC Form 1."""
    logger.info("Compiling FERC Form 1 fuel table...")
    logger.info(f"{len(pudl_out_ferc1.fuel_ferc1())} fuel records found")


@pytest.mark.parametrize(
    "cases", [
        pytest.param(pv.fuel_ferc1_coal_mmbtu_per_unit_bounds,
                     id="coal_mmbtu_per_unit"),
        pytest.param(pv.fuel_ferc1_oil_mmbtu_per_unit_bounds,
                     id="oil_mmbtu_per_unit"),
        pytest.param(pv.fuel_ferc1_gas_mmbtu_per_unit_bounds,
                     id="gas_mmbtu_per_unit"),
        pytest.param(pv.fuel_ferc1_coal_cost_per_mmbtu_bounds,
                     id="coal_cost_per_mmbtu"),
        pytest.param(pv.fuel_ferc1_oil_cost_per_mmbtu_bounds,
                     id="oil_cost_per_mmbtu"),
        pytest.param(pv.fuel_ferc1_gas_cost_per_mmbtu_bounds,
                     id="gas_cost_per_mmbtu"),
        pytest.param(pv.fuel_ferc1_coal_cost_per_unit_bounds,
                     id="coal_cost_per_unit"),
        pytest.param(
            pv.fuel_ferc1_oil_cost_per_unit_bounds,
            id="oil_cost_per_unit",
            marks=pytest.mark.xfail(reason="FERC 1 fuel unit errors?")
        ),
        pytest.param(pv.fuel_ferc1_gas_cost_per_unit_bounds,
                     id="gas_cost_per_unit"),
    ]
)
def test_vs_bounds(pudl_out_ferc1, live_pudl_db, cases):
    """Test distributions of reported plants_steam_ferc1 columns."""
    if not live_pudl_db:
        raise AssertionError("Data validation only works with a live PUDL DB.")
    for case in cases:
        pv.vs_bounds(pudl_out_ferc1.fuel_ferc1(), **case)


def test_self_vs_historical(pudl_out_ferc1, live_pudl_db):
    """Validate..."""
    if not live_pudl_db:
        raise AssertionError("Data validation only works with a live PUDL DB.")
    for args in pv.fuel_ferc1_self:
        pv.vs_self(pudl_out_ferc1.fuel_ferc1(), **args)
