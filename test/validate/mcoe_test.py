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
import sqlalchemy as sa

from pudl import validate as pv
from pudl.helpers import get_parquet_table

logger = logging.getLogger(__name__)


###############################################################################
# Tests that check for existence and uniqueness of some MCOE outputs:
# These tests were inspired by some bad merges that generated multiple copies
# of some records in the past...
###############################################################################
@pytest.mark.parametrize(
    "table_name",
    [
        "out_eia__yearly_generators",
        "out_eia__monthly_generators",
    ],
)
def test_no_null_cols_mcoe(
    live_dbs: bool,
    table_name: str,
    pudl_engine: sa.Engine,  # Required to ensure that the data is available.
) -> None:
    """Verify that output DataFrames have no entirely NULL columns."""
    if not live_dbs:
        pytest.skip("Data validation only works with a live PUDL DB.")

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
    df = get_parquet_table(table_name)
    cols = [col for col in df.columns if col not in deprecated_cols]
    pv.no_null_cols(df, cols=cols, df_name=table_name)
