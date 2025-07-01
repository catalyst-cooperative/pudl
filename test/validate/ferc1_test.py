"""Validate post-ETL FERC Form 1 data and the associated derived outputs."""

import logging

import pytest

from pudl import validate as pv

logger = logging.getLogger(__name__)


@pytest.mark.parametrize(
    "asset_key,cols",
    [
        ("out_ferc1__yearly_steam_plants_fuel_by_plant_sched402", "all"),
        ("out_ferc1__yearly_steam_plants_fuel_sched402", "all"),
        ("out_ferc1__yearly_plant_in_service_sched204", "all"),
        ("out_ferc1__yearly_all_plants", "all"),
        ("out_ferc1__yearly_hydroelectric_plants_sched406", "all"),
        ("out_ferc1__yearly_pumped_storage_plants_sched408", "all"),
        ("out_ferc1__yearly_small_plants_sched410", "all"),
        ("out_ferc1__yearly_steam_plants_sched402", "all"),
        ("out_ferc1__yearly_purchased_power_and_exchanges_sched326", "all"),
    ],
)
def test_no_null_cols_ferc1(live_dbs, asset_value_loader, cols, asset_key):
    """Verify that output DataFrames have no entirely NULL columns."""
    if not live_dbs:
        pytest.skip("Data validation only works with a live PUDL DB.")
    pv.no_null_cols(
        asset_value_loader.load_asset_value(asset_key), cols=cols, df_name=asset_key
    )
