"""Test MCOE (marginal cost of electricity) module functionality.

This set of test attempts to exercise all of the functions which are used in
the calculation of the marginal cost of electricity (MCOE), based on fuel
costs reported to EIA, and non-fuel operating costs reported to FERC.  Much
of what these functions do is attempt to correctly attribute data reported on a
per plant basis to individual generators.

For now, these calculations are only using the EIA fuel cost data. FERC Form 1
non-fuel production costs have yet to be integrated.
"""
import logging

import pytest

from pudl import validate as pv

logger = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def pudl_out_mcoe(pudl_out_eia, live_dbs):
    """A fixture to calculate MCOE appropriately for testing.

    By default, the MCOE calculation drops rows with "unreasonable" values for heat
    rate, fuel costs, and capacity factors. However, for the purposes of testing, we
    don't want to lose those values -- that's the kind of thing we're looking for.  So
    here we override those defaults, causing the MCOE output dataframe to be cached with
    all the nasty details, so it is available for the rest of the tests that look at
    MCOE results in this module
    """
    if live_dbs and pudl_out_eia.freq is not None:
        logger.info("Calculating MCOE, leaving in all the nasty bits.")
        _ = pudl_out_eia.mcoe(
            update=True,
            min_heat_rate=None,
            min_fuel_cost_per_mwh=None,
            min_cap_fact=None,
            max_cap_fact=None,
            all_gens=False,
        )
    return pudl_out_eia


###############################################################################
# Tests that look check for existence and uniqueness of some MCOE outputs:
# These tests were inspired by some bad merges that generated multiple copies
# of some records in the past...
###############################################################################
@pytest.mark.parametrize(
    "df_name", ["hr_by_unit", "hr_by_gen", "fuel_cost", "capacity_factor", "mcoe"]
)
def test_no_null_cols_mcoe(pudl_out_mcoe, live_dbs, df_name):
    """Verify that output DataFrames have no entirely NULL columns."""
    if not live_dbs:
        pytest.skip("Data validation only works with a live PUDL DB.")
    if pudl_out_mcoe.freq is None:
        pytest.skip()

    # These are columns that only exist in 2006 and older data, beyond the time
    # for which we can calculate the MCOE:
    deprecated_cols = [
        "distributed_generation",
        "energy_source_1_transport_1",
        "energy_source_1_transport_2",
        "energy_source_1_transport_3",
        "energy_source_2_transport_1",
        "energy_source_2_transport_2",
        "energy_source_2_transport_3",
        "owned_by_non_utility",
        "reactive_power_output_mvar",
        "summer_capacity_estimate",
        "winter_capacity_estimate",
    ]
    df = pudl_out_mcoe.__getattribute__(df_name)()
    cols = [col for col in df.columns if col not in deprecated_cols]
    pv.no_null_cols(df, cols=cols, df_name=df_name)


@pytest.mark.parametrize(
    "df_name,thresh",
    [
        pytest.param(
            "mcoe",
            0.8,
            marks=pytest.mark.xfail(
                reason="The net generation allocation is now integrated into MCOE. The "
                "allocated fuel still needs to be integrated - without it we'll have "
                "nulls. See #2033"
            ),
        ),
    ],
)
def test_no_null_rows_mcoe(pudl_out_mcoe, live_dbs, df_name, thresh):
    """Verify that output DataFrames have no overly NULL rows.

    Currently we only test the MCOE dataframe because it has lots of columns and some
    complicated merges. For tables with fewer columns, the "index" columns end up being
    most of them, and should probably be skipped.
    """
    if not live_dbs:
        pytest.skip("Data validation only works with a live PUDL DB.")
    if pudl_out_mcoe.freq is None:
        pytest.skip()

    pv.no_null_rows(
        df=pudl_out_mcoe.__getattribute__(df_name)(),
        df_name=df_name,
        thresh=thresh,
    )


@pytest.mark.parametrize(
    "df_name,monthly_rows,annual_rows",
    [
        ("hr_by_unit", 362_011, 30_309),
        ("hr_by_gen", 554_665, 46_370),
        ("fuel_cost", 554_665, 46_370),
        ("capacity_factor", 5_171_497, 432_570),
        ("mcoe", 5_171_881, 432_602),
    ],
)
def test_minmax_rows_mcoe(pudl_out_mcoe, live_dbs, monthly_rows, annual_rows, df_name):
    """Verify that output DataFrames don't have too many or too few rows."""
    if not live_dbs:
        pytest.skip("Data validation only works with a live PUDL DB.")
    if pudl_out_mcoe.freq is None:
        pytest.skip()
    if pudl_out_mcoe.freq == "MS":
        expected_rows = monthly_rows
    else:
        assert pudl_out_mcoe.freq == "AS"
        expected_rows = annual_rows
    _ = (
        pudl_out_mcoe.__getattribute__(df_name)()
        .pipe(
            pv.check_min_rows, expected_rows=expected_rows, margin=0.0, df_name=df_name
        )
        .pipe(
            pv.check_max_rows, expected_rows=expected_rows, margin=0.0, df_name=df_name
        )
    )


@pytest.mark.parametrize(
    "df_name,unique_subset",
    [
        ("hr_by_unit", ["report_date", "plant_id_eia", "unit_id_pudl"]),
        ("hr_by_gen", ["report_date", "plant_id_eia", "generator_id"]),
        ("fuel_cost", ["report_date", "plant_id_eia", "generator_id"]),
        ("capacity_factor", ["report_date", "plant_id_eia", "generator_id"]),
        ("mcoe", ["report_date", "plant_id_eia", "generator_id"]),
    ],
)
def test_unique_rows_mcoe(pudl_out_mcoe, live_dbs, unique_subset, df_name):
    """Test whether dataframe has unique records within a subset of columns."""
    if not live_dbs:
        pytest.skip("Data validation only works with a live PUDL DB.")
    if pudl_out_mcoe.freq is None:
        pytest.skip()
    pv.check_unique_rows(
        pudl_out_mcoe.__getattribute__(df_name)(), subset=unique_subset, df_name=df_name
    )


###############################################################################
# Tests that look at distributions of MCOE calculation outputs.
###############################################################################


@pytest.mark.parametrize("fuel,max_idle", [("gas", 0.15), ("coal", 0.075)])
def test_idle_capacity(fuel, max_idle, pudl_out_mcoe, live_dbs):
    """Validate that idle capacity isn't tooooo high."""
    if not live_dbs:
        pytest.skip("Data validation only works with a live PUDL DB.")
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


def test_gas_capacity_factor(pudl_out_mcoe, live_dbs):
    """Validate Coal Capacity Factors are within reasonable limits."""
    if not live_dbs:
        pytest.skip("Data validation only works with a live PUDL DB.")
    if pudl_out_mcoe.freq is None:
        pytest.skip()
    for args in pv.mcoe_gas_capacity_factor:
        pv.vs_bounds(pudl_out_mcoe.mcoe(), **args)


def test_coal_capacity_factor(pudl_out_mcoe, live_dbs):
    """Validate Coal Capacity Factors are within reasonable limits."""
    if not live_dbs:
        pytest.skip("Data validation only works with a live PUDL DB.")
    if pudl_out_mcoe.freq is None:
        pytest.skip()
    for args in pv.mcoe_coal_capacity_factor:
        pv.vs_bounds(pudl_out_mcoe.mcoe(), **args)


def test_gas_heat_rate_by_unit(pudl_out_mcoe, live_dbs):
    """Validate Coal Capacity Factors are within reasonable limits."""
    if not live_dbs:
        pytest.skip("Data validation only works with a live PUDL DB.")
    if pudl_out_mcoe.freq is None:
        pytest.skip()
    for args in pv.mcoe_gas_heat_rate:
        pv.vs_bounds(pudl_out_mcoe.mcoe(), **args)


def test_coal_heat_rate_by_unit(pudl_out_mcoe, live_dbs):
    """Validate Coal Capacity Factors are within reasonable limits."""
    if not live_dbs:
        pytest.skip("Data validation only works with a live PUDL DB.")
    if pudl_out_mcoe.freq is None:
        pytest.skip()
    for args in pv.mcoe_coal_heat_rate:
        pv.vs_bounds(pudl_out_mcoe.mcoe(), **args)


def test_fuel_cost_per_mwh(pudl_out_mcoe, live_dbs):
    """Verify that fuel costs per MWh are reasonable for coal & gas."""
    if not live_dbs:
        pytest.skip("Data validation only works with a live PUDL DB.")
    # The annual numbers for MCOE costs have too many NA values:
    if pudl_out_mcoe.freq != "MS":
        pytest.skip()
    for args in pv.mcoe_self_fuel_cost_per_mwh:
        pv.vs_self(pudl_out_mcoe.mcoe(), **args)

    for args in pv.mcoe_fuel_cost_per_mwh:
        pv.vs_bounds(pudl_out_mcoe.mcoe(), **args)


def test_fuel_cost_per_mmbtu(pudl_out_mcoe, live_dbs):
    """Verify that fuel costs per mmbtu are reasonable for coal & gas."""
    if not live_dbs:
        pytest.skip("Data validation only works with a live PUDL DB.")
    # The annual numbers for MCOE costs have too many NA values:
    if pudl_out_mcoe.freq != "MS":
        pytest.skip()
    for args in pv.mcoe_self_fuel_cost_per_mmbtu:
        pv.vs_self(pudl_out_mcoe.mcoe(), **args)

    for args in pv.mcoe_fuel_cost_per_mmbtu:
        pv.vs_bounds(pudl_out_mcoe.mcoe(), **args)


def test_mcoe_self(pudl_out_mcoe, live_dbs):
    """Test MCOE outputs against their historical selves..."""
    if not live_dbs:
        pytest.skip("Data validation only works with a live PUDL DB.")
    if pudl_out_mcoe.freq is None:
        pytest.skip()
    for args in pv.mcoe_self:
        pv.vs_self(pudl_out_mcoe.mcoe(), **args)
