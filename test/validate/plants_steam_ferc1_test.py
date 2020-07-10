"""
Validate post-ETL FERC Form 1 data and the associated derived outputs.

These tests depend on a FERC Form 1 specific PudlTabl output object, which is
a parameterized fixture that has session scope.
"""
import logging

import pytest

import pudl
from pudl import validate as pv

logger = logging.getLogger(__name__)


@pytest.mark.parametrize(
    "cases", [
        pytest.param(pv.plants_steam_ferc1_capacity, id="capacity"),
        pytest.param(pv.plants_steam_ferc1_expenses, id="expenses"),
        pytest.param(
            pv.plants_steam_ferc1_capacity_ratios,
            id="capacity_ratios"
        ),
        pytest.param(
            pv.plants_steam_ferc1_connected_hours,
            id="connected_hours",
            marks=pytest.mark.xfail(reason="FERC 1 data reporting errors.")
        ),
    ]
)
def test_vs_bounds(pudl_out_ferc1, live_pudl_db, cases):
    """Test distributions of reported plants_steam_ferc1 columns."""
    if not live_pudl_db:
        raise AssertionError("Data validation only works with a live PUDL DB.")
    validate_df = (
        pudl_out_ferc1.plants_steam_ferc1().
        assign(
            water_limited_ratio=lambda x: x.water_limited_capacity_mw / x.capacity_mw,
            not_water_limited_ratio=lambda x: x.not_water_limited_capacity_mw / x.capacity_mw,
            peak_demand_ratio=lambda x: x.peak_demand_mw / x.capacity_mw,
            capability_ratio=lambda x: x.plant_capability_mw / x.capacity_mw,
        )
    )
    for case in cases:
        pudl.validate.vs_bounds(validate_df, **case)


def test_self_vs_historical(pudl_out_ferc1, live_pudl_db):
    """Validate..."""
    if not live_pudl_db:
        raise AssertionError("Data validation only works with a live PUDL DB.")
    validate_df = (
        pudl_out_ferc1.plants_steam_ferc1().
        assign(
            water_limited_ratio=lambda x: x.water_limited_capacity_mw / x.capacity_mw,
            not_water_limited_ratio=lambda x: x.not_water_limited_capacity_mw / x.capacity_mw,
            peak_demand_ratio=lambda x: x.peak_demand_mw / x.capacity_mw,
            capability_ratio=lambda x: x.plant_capability_mw / x.capacity_mw,
        )
    )
    for args in pv.plants_steam_ferc1_self:
        pudl.validate.vs_self(validate_df, **args)


@pytest.mark.xfail(reason="Known duplicates need to be debugged.")
def test_dupe_years_in_plant_id_ferc1(pudl_out_ferc1):
    """
    Test that we have no duplicate years within any plant_id_ferc1.

    Test to make sure that we don't have any plant_id_ferc1 time series
    which include more than one record from a given year. Fail the test
    if we find such cases (which... we do, as of writing).
    """
    steam_df = pudl_out_ferc1.plants_steam_ferc1()
    year_dupes = (
        steam_df.
        groupby(['plant_id_ferc1', 'report_year'])['utility_id_ferc1'].
        count().
        reset_index().
        rename(columns={'utility_id_ferc1': 'year_dupes'}).
        query('year_dupes>1')
    )
    for dupe in year_dupes.itertuples():
        logger.error(
            f"Found report_year={dupe.report_year} "
            f"{dupe.year_dupes} times in "
            f"plant_id_ferc1={dupe.plant_id_ferc1}"
        )
    if len(year_dupes) != 0:
        raise AssertionError(
            f"Found {len(year_dupes)} duplicate years in FERC1 "
            f"plant ID time series"
        )


@pytest.mark.xfail(reason="One known ID inconsistency to be debugged.")
def test_plant_id_clash(pudl_out_ferc1):
    """
    Test for FERC & PUDL Plant ID consistency.

    Each PUDL Plant ID may contain several FERC Plant IDs, but one FERC Plant
    ID should only ever appear within a single PUDL Plant ID. Test this
    assertion and fail if it is untrue (as... we know it is right now).
    """
    steam_df = pudl_out_ferc1.plants_steam_ferc1()
    bad_plant_ids_ferc1 = (
        steam_df[['plant_id_pudl', 'plant_id_ferc1']].
        drop_duplicates().
        groupby('plant_id_ferc1').
        count().
        rename(columns={'plant_id_pudl': 'pudl_id_count'}).
        query('pudl_id_count>1').
        reset_index().
        plant_id_ferc1.values.tolist()
    )
    if bad_plant_ids_ferc1:
        bad_records = steam_df[steam_df.plant_id_ferc1.
                               isin(bad_plant_ids_ferc1)]
        bad_plant_ids_pudl = bad_records.plant_id_pudl.unique().tolist()
        raise AssertionError(
            f"Found {len(bad_plant_ids_ferc1)} plant_id_ferc1 values "
            f"associated with {len(bad_plant_ids_pudl)} non-unique "
            f"plant_id_pudl values.\nplant_id_ferc1: {bad_plant_ids_ferc1}\n"
            f"plant_id_pudl: {bad_plant_ids_pudl}."
        )
