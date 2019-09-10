"""
Test MCOE (marginal cost of electricity) module functionality.

This set of test attempts to exercise all of the functions which are used in
the calculation of the marginal cost of electricity (MCOE), based on fuel
costs reported to EIA, and non-fuel operating costs reported to FERC.  Much
of what these functions do is attempt to correctly attribute data reported on a
per plant basis to individual generators.

For now, these calculations are only using the EIA fuel cost data. FERC Form 1
non-fuel production costs have yet to be integrated.
"""
import logging

import pandas as pd
import pytest

import pudl

logger = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def pudl_out_mcoe(pudl_out_eia, live_pudl_db):
    """
    A fixture to calculate MCOE appropriately for testing.

    By default, the MCOE calculation drops rows with "unreasonable" values for
    heat rate, fuel costs, and capacity factors. However, for the purposes of
    testing, we don't want to lose those values -- that's the kind of thing
    we're looking for.  So here we override those defaults, causing the MCOE
    output dataframe to be cached with all the nasty details, so it is
    available for the rest of the tests that look at MCOE results in this
    module

    """
    logger.info("Calculating MCOE, leaving in all the nasty bits.")
    _ = pudl_out_eia.mcoe(
        update=True,
        min_heat_rate=None,
        min_fuel_cost_per_mwh=None,
        min_cap_fact=None,
        max_cap_fact=None
    )
    return pudl_out_eia


def test_bga(pudl_out_eia, live_pudl_db):
    """Test the boiler generator associations."""
    if not live_pudl_db:
        raise AssertionError("Data validation only works with a live PUDL DB.")

    logger.info("Inferring complete boiler-generator associations...")
    bga = pudl_out_eia.bga()
    gens_simple = pudl_out_eia.gens_eia860()[['report_date',
                                              'plant_id_eia',
                                              'generator_id',
                                              'fuel_type_code_pudl']]
    bga_gens = bga[['report_date', 'plant_id_eia',
                    'unit_id_pudl', 'generator_id']].drop_duplicates()

    gens_simple = pd.merge(gens_simple, bga_gens,
                           on=['report_date', 'plant_id_eia', 'generator_id'],
                           validate='one_to_one')
    units_simple = gens_simple.drop('generator_id', axis=1).drop_duplicates()
    units_fuel_count = \
        units_simple.groupby(
            ['report_date',
             'plant_id_eia',
             'unit_id_pudl'])['fuel_type_code_pudl'].count().reset_index()
    units_fuel_count.rename(
        columns={'fuel_type_code_pudl': 'fuel_type_count'}, inplace=True)
    units_simple = pd.merge(units_simple, units_fuel_count,
                            on=['report_date', 'plant_id_eia', 'unit_id_pudl'])
    num_multi_fuel_units = len(units_simple[units_simple.fuel_type_count > 1])
    multi_fuel_unit_fraction = num_multi_fuel_units / len(units_simple)
    logger.warning(
        f"{multi_fuel_unit_fraction:.0%} of generation units contain "
        f"generators with differing primary fuels.")


###############################################################################
# Tests that look check for existence and uniqueness of some MCOE outputs:
# These tests were inspired by some bad merges that generated multiple copies
# of some records in the past...
###############################################################################


def test_hr_by_unit_exists(pudl_out_eia, live_pudl_db):
    """Calculate heat rates on a per generation unit basis."""
    if not live_pudl_db:
        raise AssertionError("Data validation only works with a live PUDL DB.")

    logger.info("Calculating heat rates by generation unit...")
    hr_by_unit = pudl_out_eia.hr_by_unit()
    logger.info(f"{len(hr_by_unit)} generation unit heat rates calculated.")
    key_cols = ['report_date', 'plant_id_eia', 'unit_id_pudl']
    if not single_records(hr_by_unit, key_cols=key_cols):
        raise AssertionError("Found non-unique unit heat rates!")


def test_hr_by_gen_unique(pudl_out_eia, live_pudl_db):
    """Calculate heat reates on a per-generator basis."""
    if not live_pudl_db:
        raise AssertionError("Data validation only works with a live PUDL DB.")

    logger.info("Calculating heat rates for individual generators...")
    hr_by_gen = pudl_out_eia.hr_by_gen()
    logger.info(f"{len(hr_by_gen)} generator heat rates calculated.")
    if not single_records(hr_by_gen):
        raise AssertionError("Found non-unique generator heat rates!")


def test_fuel_cost_unique(pudl_out_eia, live_pudl_db):
    """Calculate fuel costs on a per-generator basis, and sanity check."""
    if not live_pudl_db:
        raise AssertionError("Data validation only works with a live PUDL DB.")
    logger.info("Calculating fuel costs by individual generator...")
    fc = pudl_out_eia.fuel_cost()
    logger.info(f"{len(fc)} per-generator fuel costs calculated.")
    if not single_records(fc):
        raise AssertionError("Found non-unique generator fuel cost records!")

###############################################################################
# Tests that look at distributions of MCOE calculation outputs.
###############################################################################


@pytest.mark.parametrize("fuel,max_idle", [('gas', 0.15), ('coal', 0.075)])
def test_idle_capacity(fuel, max_idle, pudl_out_mcoe, live_pudl_db):
    """Validate that idle capacity isn't tooooo high."""
    if not live_pudl_db:
        raise AssertionError("Data validation only works with a live PUDL DB.")

    mcoe_tmp = pudl_out_mcoe.mcoe().query(f"fuel_type_code_pudl=='{fuel}'")
    nonzero_cf = mcoe_tmp[mcoe_tmp.capacity_factor != 0.0]
    working_capacity = nonzero_cf.capacity_mw.sum()
    total_capacity = mcoe_tmp.capacity_mw.sum()
    idle_capacity = 1.0 - (working_capacity / total_capacity)
    logger.info(f"Idle {fuel} capacity: {idle_capacity:.2%}")

    if idle_capacity > max_idle:
        raise AssertionError(f"Idle capacity ({idle_capacity}) is too high.")


def test_gas_capacity_factor(pudl_out_mcoe, live_pudl_db):
    """Validate Coal Capacity Factors are within reasonable limits."""
    if not live_pudl_db:
        raise AssertionError("Data validation only works with a live PUDL DB.")
    for args in pudl.validate.mcoe_gas_capacity_factor:
        pudl.validate.vs_bounds(pudl_out_mcoe.mcoe(), **args)


def test_coal_capacity_factor(pudl_out_mcoe, live_pudl_db):
    """Validate Coal Capacity Factors are within reasonable limits."""
    if not live_pudl_db:
        raise AssertionError("Data validation only works with a live PUDL DB.")
    for args in pudl.validate.mcoe_coal_capacity_factor:
        pudl.validate.vs_bounds(pudl_out_mcoe.mcoe(), **args)


def test_gas_heat_rate_by_unit(pudl_out_mcoe, live_pudl_db):
    """Validate Coal Capacity Factors are within reasonable limits."""
    if not live_pudl_db:
        raise AssertionError("Data validation only works with a live PUDL DB.")
    for args in pudl.validate.mcoe_gas_heat_rate:
        pudl.validate.vs_bounds(pudl_out_mcoe.mcoe(), **args)


def test_coal_heat_rate_by_unit(pudl_out_mcoe, live_pudl_db):
    """Validate Coal Capacity Factors are within reasonable limits."""
    if not live_pudl_db:
        raise AssertionError("Data validation only works with a live PUDL DB.")
    for args in pudl.validate.mcoe_coal_heat_rate:
        pudl.validate.vs_bounds(pudl_out_mcoe.mcoe(), **args)


def test_fuel_cost_per_mwh(pudl_out_mcoe, live_pudl_db):
    """Verify that fuel costs per MWh are reasonable for coal & gas."""
    if not live_pudl_db:
        raise AssertionError("Data validation only works with a live PUDL DB.")
    # The annual numbers for MCOE costs have too many NA values:
    if pudl_out_mcoe.freq == "AS":
        pytest.skip()
    for args in pudl.validate.mcoe_self_fuel_cost_per_mwh:
        pudl.validate.vs_self(pudl_out_mcoe.mcoe(), **args)

    for args in pudl.validate.mcoe_fuel_cost_per_mwh:
        pudl.validate.vs_bounds(pudl_out_mcoe.mcoe(), **args)


def test_fuel_cost_per_mmbtu(pudl_out_mcoe, live_pudl_db):
    """Verify that fuel costs per mmbtu are reasonable for coal & gas."""
    if not live_pudl_db:
        raise AssertionError("Data validation only works with a live PUDL DB.")
    # The annual numbers for MCOE costs have too many NA values:
    if pudl_out_mcoe.freq == "AS":
        pytest.skip()
    for args in pudl.validate.mcoe_self_fuel_cost_per_mmbtu:
        pudl.validate.vs_self(pudl_out_mcoe.mcoe(), **args)

    for args in pudl.validate.mcoe_fuel_cost_per_mmbtu:
        pudl.validate.vs_bounds(pudl_out_mcoe.mcoe(), **args)


def test_mcoe_self(pudl_out_mcoe, live_pudl_db):
    """Test MCOE outputs against their historical selves..."""
    if not live_pudl_db:
        raise AssertionError("Data validation only works with a live PUDL DB.")
    for args in pudl.validate.mcoe_self:
        pudl.validate.vs_self(pudl_out_mcoe.mcoe(), **args)

###############################################################################
# Helper functions for the above tests.
###############################################################################


def single_records(df,
                   key_cols=['report_date', 'plant_id_eia', 'generator_id']):
    """Test whether dataframe has a single record per generator."""
    len_1 = len(df)
    len_2 = len(df.drop_duplicates(subset=key_cols))
    return bool(len_1 == len_2)


def nonunique_gens(df,
                   key_cols=['plant_id_eia', 'generator_id', 'report_date']):
    """Generate a list of all the non-unique generator records for testing."""
    unique_gens = df.drop_duplicates(subset=key_cols)
    dupes = df[~df.isin(unique_gens)].dropna()
    dupes = dupes.sort_values(by=key_cols)
    return dupes
