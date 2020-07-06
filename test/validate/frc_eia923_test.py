"""Validate post-ETL Fuel Receipts and Costs data from EIA 923."""
import logging

import pytest

import pudl
from pudl import validate as pv

logger = logging.getLogger(__name__)


@pytest.mark.parametrize(
    "cases", [
        pytest.param(pv.frc_eia923_coal_mercury_content, id="coal_mercury"),
        pytest.param(pv.frc_eia923_coal_sulfur_content,
                     id="coal_sulfur_content"),
        pytest.param(pv.frc_eia923_coal_ash_content, id="coal_ash_content"),
        pytest.param(pv.frc_eia923_coal_moisture_content,
                     id="coal_moisture_content"),
        pytest.param(pv.frc_eia923_coal_ant_heat_content,
                     id="coal_ant_heat_content"),
        pytest.param(pv.frc_eia923_coal_bit_heat_content,
                     id="coal_bit_heat_content"),
        pytest.param(pv.frc_eia923_coal_sub_heat_content,
                     id="coal_sub_heat_content"),
        pytest.param(pv.frc_eia923_coal_lig_heat_content,
                     id="coal_lig_heat_content"),
        pytest.param(pv.frc_eia923_oil_dfo_heat_content,
                     id="oil_dfo_heat_content"),
        pytest.param(pv.frc_eia923_gas_sgc_heat_content,
                     id="gas_sgc_heat_content"),
        pytest.param(pv.frc_eia923_oil_jf_heat_content,
                     id="oil_jf_heat_content"),
        pytest.param(pv.frc_eia923_oil_ker_heat_content,
                     id="oil_ker_heat_content"),
        pytest.param(pv.frc_eia923_petcoke_heat_content,
                     id="petcoke_heat_content"),
        pytest.param(pv.frc_eia923_rfo_heat_content, id="rfo_heat_content"),
        pytest.param(pv.frc_eia923_propane_heat_content,
                     id="propane_heat_content"),
        pytest.param(pv.frc_eia923_petcoke_syngas_heat_content,
                     id="petcoke_syngas_heat_content"),
        pytest.param(pv.frc_eia923_waste_oil_heat_content,
                     id="waste_oil_heat_content"),
        pytest.param(pv.frc_eia923_blast_furnace_gas_heat_content,
                     id="blast_furnace_gas_heat_content"),
        pytest.param(pv.frc_eia923_natural_gas_heat_content,
                     id="natural_gas_heat_content"),
        pytest.param(pv.frc_eia923_other_gas_heat_content,
                     id="other_gas_heat_content"),
        pytest.param(pv.frc_eia923_ag_byproduct_heat_content,
                     id="ag_byproduct_heat_content"),
        pytest.param(pv.frc_eia923_muni_solids_heat_content,
                     id="muni_solids_heat_content"),
        pytest.param(pv.frc_eia923_wood_solids_heat_content,
                     id="wood_solids_heat_content"),
        pytest.param(pv.frc_eia923_biomass_solids_heat_content,
                     id="biomass_solids_heat_content"),
        pytest.param(pv.frc_eia923_biomass_liquids_heat_content,
                     id="biomass_liquids_heat_content"),
        pytest.param(pv.frc_eia923_biomass_gas_heat_content,
                     id="biomass_gas_heat_content"),
        pytest.param(pv.frc_eia923_sludge_heat_content,
                     id="sludge_heat_content"),
        pytest.param(pv.frc_eia923_black_liquor_heat_content,
                     id="black_liquor_heat_content"),
        pytest.param(pv.frc_eia923_wood_liquids_heat_content,
                     id="wood_liquids_heat_content"),
        pytest.param(pv.frc_eia923_landfill_gas_heat_content,
                     id="landfill_gas_heat_content"),
    ]
)
def test_vs_bounds(pudl_out_eia, live_pudl_db, cases):
    """Validate distribution of reported data is within expected bounds."""
    if not live_pudl_db:
        raise AssertionError("Data validation only works with a live PUDL DB.")
    if pudl_out_eia.freq is not None:
        pytest.skip("Test only runs on un-aggregated data.")

    for case in cases:
        pudl.validate.vs_bounds(pudl_out_eia.frc_eia923(), **case)


def test_self_vs_historical(pudl_out_eia, live_pudl_db):
    """Validate the whole dataset against historical annual subsamples."""
    if not live_pudl_db:
        raise AssertionError("Data validation only works with a live PUDL DB.")
    if pudl_out_eia.freq is not None:
        pytest.skip("Test only runs on un-aggregated data.")

    for args in pudl.validate.frc_eia923_self:
        pudl.validate.vs_self(pudl_out_eia.frc_eia923(), **args)


def test_agg_vs_historical(pudl_out_orig, pudl_out_eia, live_pudl_db):
    """Validate whole dataset against aggregated historical values."""
    if not live_pudl_db:
        raise AssertionError("Data validation only works with a live PUDL DB.")
    if pudl_out_eia.freq is None:
        pytest.skip("Only run if pudl_out_eia != pudl_out_orig.")

    for args in pudl.validate.frc_eia923_agg:
        pudl.validate.vs_historical(pudl_out_orig.frc_eia923(),
                                    pudl_out_eia.frc_eia923(),
                                    **args)
