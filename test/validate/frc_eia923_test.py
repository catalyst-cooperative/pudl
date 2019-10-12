"""Validate post-ETL Fuel Receipts and Costs data from EIA 923."""

import logging

import pytest

import pudl
import pudl.validate as pv

logger = logging.getLogger(__name__)


@pytest.mark.parametrize(
    "cases", [
        pytest.param(
            pv.frc_eia923_coal_mercury_content,
            id="coal_mercury",
            marks=pytest.mark.xfail(reason="FERC 1 data reporting errors.")
        ),
        pytest.param(pv.frc_eia923_coal_heat_content, id="coal_heat_content"),
        pytest.param(pv.frc_eia923_coal_ash_content, id="coal_ash_content"),
        pytest.param(
            pv.frc_eia923_coal_moisture_content, id="coal_moisture_content"),
        pytest.param(
            pv.frc_eia923_coal_sulfur_content, id="coal_sulfur_content"),
        pytest.param(pv.frc_eia923_oil_heat_content, id="oil_heat_content"),
        pytest.param(
            pv.frc_eia923_gas_heat_content,
            id="gas_heat_content",
            marks=pytest.mark.xfail(reason="FERC 1 data reporting errors.")
        ),
    ]
)
def test_vs_bounds(pudl_out_orig, live_pudl_db, cases):
    """Validate distribution of reported data is within expected bounds."""
    if not live_pudl_db:
        raise AssertionError("Data validation only works with a live PUDL DB.")
    for case in cases:
        pudl.validate.vs_bounds(pudl_out_orig.frc_eia923(), **case)


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
