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

from pudl import validate as pv

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
    if pudl_out_eia.freq is not None:
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
@pytest.mark.parametrize(
    "df_name,cols", [
        ("hr_by_unit", "all"),
        ("hr_by_gen", "all"),
        ("fuel_cost", "all"),
        ("capacity_factor", "all"),
        ("bga", "all"),
        ("mcoe", "all"),
    ]
)
def test_no_null_cols_mcoe(pudl_out_mcoe, live_pudl_db, cols, df_name):
    """Verify that output DataFrames have no entirely NULL columns."""
    if not live_pudl_db:
        raise AssertionError("Data validation only works with a live PUDL DB.")
    if pudl_out_mcoe.freq is None:
        pytest.skip()

    pv.no_null_cols(
        pudl_out_mcoe.__getattribute__(df_name)(),
        cols=cols, df_name=df_name)


@pytest.mark.parametrize(
    "df_name,monthly_rows,annual_rows", [
        ("bga", 90_127, 90_127),
        ("hr_by_unit", 277_848, 23_154),
        ("hr_by_gen", 407_424, 33_952),
        ("fuel_cost", 447_932, 33_952),
        ("capacity_factor", 429_072, 35_756),
        ("mcoe", 429_320, 36_004),
    ])
def test_minmax_rows_mcoe(pudl_out_mcoe, live_pudl_db,
                          monthly_rows, annual_rows, df_name):
    """Verify that output DataFrames don't have too many or too few rows."""
    if not live_pudl_db:
        raise AssertionError("Data validation only works with a live PUDL DB.")
    if pudl_out_mcoe.freq is None:
        pytest.skip()
    if (pudl_out_mcoe.freq == "MS"):
        expected_rows = monthly_rows
    else:
        assert pudl_out_mcoe.freq == "AS"
        expected_rows = annual_rows
    _ = (
        pudl_out_mcoe.__getattribute__(df_name)()
        .pipe(pv.check_min_rows, expected_rows=expected_rows,
              margin=0.05, df_name=df_name)
        .pipe(pv.check_max_rows, expected_rows=expected_rows,
              margin=0.05, df_name=df_name)
    )


@pytest.mark.parametrize(
    "df_name,unique_subset", [
        ("bga", ["report_date", "plant_id_eia", "boiler_id", "generator_id"]),
        ("hr_by_unit", ["report_date", "plant_id_eia", "unit_id_pudl"]),
        ("hr_by_gen", ["report_date", "plant_id_eia", "generator_id"]),
        ("fuel_cost", ["report_date", "plant_id_eia", "generator_id"]),
        ("capacity_factor", ["report_date", "plant_id_eia", "generator_id"]),
        ("mcoe", ["report_date", "plant_id_eia", "generator_id"]),
    ])
def test_unique_rows_mcoe(pudl_out_mcoe, live_pudl_db, unique_subset, df_name):
    """Test whether dataframe has unique records within a subset of columns."""
    if not live_pudl_db:
        raise AssertionError("Data validation only works with a live PUDL DB.")
    if pudl_out_mcoe.freq is None:
        pytest.skip()
    pv.check_unique_rows(
        pudl_out_mcoe.__getattribute__(df_name)(),
        subset=unique_subset, df_name=df_name)

###############################################################################
# Tests that look at distributions of MCOE calculation outputs.
###############################################################################


@pytest.mark.parametrize("fuel,max_idle", [('gas', 0.15), ('coal', 0.075)])
def test_idle_capacity(fuel, max_idle, pudl_out_mcoe, live_pudl_db):
    """Validate that idle capacity isn't tooooo high."""
    if not live_pudl_db:
        raise AssertionError("Data validation only works with a live PUDL DB.")
    if pudl_out_mcoe.freq is None:
        pytest.skip()

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
    if pudl_out_mcoe.freq is None:
        pytest.skip()
    for args in pv.mcoe_gas_capacity_factor:
        pv.vs_bounds(pudl_out_mcoe.mcoe(), **args)


def test_coal_capacity_factor(pudl_out_mcoe, live_pudl_db):
    """Validate Coal Capacity Factors are within reasonable limits."""
    if not live_pudl_db:
        raise AssertionError("Data validation only works with a live PUDL DB.")
    if pudl_out_mcoe.freq is None:
        pytest.skip()
    for args in pv.mcoe_coal_capacity_factor:
        pv.vs_bounds(pudl_out_mcoe.mcoe(), **args)


def test_gas_heat_rate_by_unit(pudl_out_mcoe, live_pudl_db):
    """Validate Coal Capacity Factors are within reasonable limits."""
    if not live_pudl_db:
        raise AssertionError("Data validation only works with a live PUDL DB.")
    if pudl_out_mcoe.freq is None:
        pytest.skip()
    for args in pv.mcoe_gas_heat_rate:
        pv.vs_bounds(pudl_out_mcoe.mcoe(), **args)


def test_coal_heat_rate_by_unit(pudl_out_mcoe, live_pudl_db):
    """Validate Coal Capacity Factors are within reasonable limits."""
    if not live_pudl_db:
        raise AssertionError("Data validation only works with a live PUDL DB.")
    if pudl_out_mcoe.freq is None:
        pytest.skip()
    for args in pv.mcoe_coal_heat_rate:
        pv.vs_bounds(pudl_out_mcoe.mcoe(), **args)


def test_fuel_cost_per_mwh(pudl_out_mcoe, live_pudl_db):
    """Verify that fuel costs per MWh are reasonable for coal & gas."""
    if not live_pudl_db:
        raise AssertionError("Data validation only works with a live PUDL DB.")
    # The annual numbers for MCOE costs have too many NA values:
    if pudl_out_mcoe.freq != "MS":
        pytest.skip()
    for args in pv.mcoe_self_fuel_cost_per_mwh:
        pv.vs_self(pudl_out_mcoe.mcoe(), **args)

    for args in pv.mcoe_fuel_cost_per_mwh:
        pv.vs_bounds(pudl_out_mcoe.mcoe(), **args)


def test_fuel_cost_per_mmbtu(pudl_out_mcoe, live_pudl_db):
    """Verify that fuel costs per mmbtu are reasonable for coal & gas."""
    if not live_pudl_db:
        raise AssertionError("Data validation only works with a live PUDL DB.")
    # The annual numbers for MCOE costs have too many NA values:
    if pudl_out_mcoe.freq != "MS":
        pytest.skip()
    for args in pv.mcoe_self_fuel_cost_per_mmbtu:
        pv.vs_self(pudl_out_mcoe.mcoe(), **args)

    for args in pv.mcoe_fuel_cost_per_mmbtu:
        pv.vs_bounds(pudl_out_mcoe.mcoe(), **args)


def test_mcoe_self(pudl_out_mcoe, live_pudl_db):
    """Test MCOE outputs against their historical selves..."""
    if not live_pudl_db:
        raise AssertionError("Data validation only works with a live PUDL DB.")
    if pudl_out_mcoe.freq is None:
        pytest.skip()
    for args in pv.mcoe_self:
        pv.vs_self(pudl_out_mcoe.mcoe(), **args)
