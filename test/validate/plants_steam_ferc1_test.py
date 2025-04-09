"""Validate post-ETL FERC Form 1 data and the associated derived outputs.

These tests depend on a FERC Form 1 specific PudlTabl output object, which is a
parameterized fixture that has session scope.
"""

import logging

import pandas as pd
import pytest

import pudl
from pudl import validate as pv

logger = logging.getLogger(__name__)


@pytest.mark.parametrize(
    "cases",
    [
        pytest.param(
            pv.core_ferc1__yearly_steam_plants_sched402_capacity, id="capacity"
        ),
        pytest.param(
            pv.core_ferc1__yearly_steam_plants_sched402_expenses, id="expenses"
        ),
        pytest.param(
            pv.core_ferc1__yearly_steam_plants_sched402_capacity_ratios,
            id="capacity_ratios",
        ),
        pytest.param(
            pv.core_ferc1__yearly_steam_plants_sched402_connected_hours,
            id="connected_hours",
            marks=pytest.mark.xfail(reason="FERC 1 data reporting errors."),
        ),
    ],
)
def test_vs_bounds(pudl_out_ferc1, live_dbs, cases):
    """Test distributions of reported core_ferc1__yearly_steam_plants_sched402 columns."""
    if not live_dbs:
        pytest.skip("Data validation only works with a live PUDL DB.")
    validate_df = pd.read_sql(
        "out_ferc1__yearly_steam_plants_sched402", pudl_out_ferc1.pudl_engine
    ).assign(
        water_limited_ratio=lambda x: x.water_limited_capacity_mw / x.capacity_mw,
        not_water_limited_ratio=lambda x: x.not_water_limited_capacity_mw
        / x.capacity_mw,
        peak_demand_ratio=lambda x: x.peak_demand_mw / x.capacity_mw,
        capability_ratio=lambda x: x.plant_capability_mw / x.capacity_mw,
    )
    for case in cases:
        pudl.validate.vs_bounds(validate_df, **case)
