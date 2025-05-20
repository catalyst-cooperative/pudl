"""Test MCOE (marginal cost of electricity) module functionality.

This set of test attempts to exercise all of the functions which are used in the
calculation of the marginal cost of electricity (MCOE), based on fuel costs reported to
EIA, and non-fuel operating costs reported to FERC.  Much of what these functions do is
attempt to correctly attribute data reported on a per plant basis to individual
generators.

For now, these calculations are only using the EIA fuel cost data. FERC Form 1 non-fuel
production costs have yet to be integrated.
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
        logger.info("Reading MCOE data (with generator attributes) out of the PUDL DB.")
        _ = pudl_out_eia.mcoe_generators()
    return pudl_out_eia


###############################################################################
# Tests that look check for existence and uniqueness of some MCOE outputs:
# These tests were inspired by some bad merges that generated multiple copies
# of some records in the past...
###############################################################################
@pytest.mark.parametrize(
    "df_name",
    [
        "hr_by_unit",
        "hr_by_gen",
        # "fuel_cost",
        "capacity_factor",
        "mcoe",
    ],
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


@pytest.mark.parametrize("df_name,thresh", [("mcoe", 0.8)])
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
