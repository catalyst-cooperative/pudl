"""Validate post-ETL FERC Form 1 data and the associated derived outputs.

These tests depend on a FERC Form 1 specific PudlTabl output object, which is a
parameterized fixture that has session scope.
"""

import logging
from itertools import chain

import pandas as pd
import pytest

from pudl import validate as pv
from pudl.etl import defs
from pudl.extract.ferc1 import TABLE_NAME_MAP_FERC1
from pudl.metadata.classes import DataSource
from pudl.transform.ferc import filter_for_freshest_data_xbrl, get_primary_key_raw_xbrl

logger = logging.getLogger(__name__)

# These are tables for which individual records have been sliced up and
# turned into columns (except for the transmission table) -- so there's no universally
# unique record ID. But we should parameterize the has_unique_record_ids class
# attributes in the FERC classes.
non_unique_record_id_tables = [
    "core_ferc1__yearly_plant_in_service_sched204",
    "core_ferc1__yearly_purchased_power_and_exchanges_sched326",
    "core_ferc1__yearly_energy_sources_sched401",
    "core_ferc1__yearly_energy_dispositions_sched401",
    "core_ferc1__yearly_utility_plant_summary_sched200",
    "core_ferc1__yearly_transmission_lines_sched422",
    "core_ferc1__yearly_balance_sheet_liabilities_sched110",
    "core_ferc1__yearly_balance_sheet_assets_sched110",
    "core_ferc1__yearly_income_statements_sched114",
    "core_ferc1__yearly_depreciation_summary_sched336",
    "core_ferc1__yearly_depreciation_changes_sched219",
    "core_ferc1__yearly_depreciation_by_function_sched219",
    "core_ferc1__yearly_operating_expenses_sched320",
    "core_ferc1__yearly_cash_flows_sched120",
    "core_ferc1__yearly_retained_earnings_sched118",
    "core_ferc1__yearly_operating_revenues_sched300",
    "core_ferc1__yearly_other_regulatory_liabilities_sched278",
    "core_ferc1__yearly_sales_by_rate_schedules_sched304",
]
unique_record_tables = [
    t
    for t in DataSource.from_id("ferc1").get_resource_ids()
    if t not in non_unique_record_id_tables
]


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
        ("_out_ferc1__yearly_plants_utilities", "all"),
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


@pytest.mark.parametrize(
    "asset_key,expected_rows",
    [
        ("_out_ferc1__yearly_plants_utilities", 7_887),
        ("out_ferc1__yearly_all_plants", 58_520),
        ("out_ferc1__yearly_balance_sheet_assets_sched110", 278_789),
        ("out_ferc1__yearly_balance_sheet_liabilities_sched110", 233_383),
        ("out_ferc1__yearly_cash_flows_sched120", 306_837),
        ("out_ferc1__yearly_depreciation_by_function_sched219", 148_352),
        ("out_ferc1__yearly_depreciation_changes_sched219", 263_942),
        ("out_ferc1__yearly_depreciation_summary_sched336", 216_710),
        ("out_ferc1__yearly_energy_dispositions_sched401", 25_954),
        ("out_ferc1__yearly_energy_sources_sched401", 38_315),
        ("out_ferc1__yearly_hydroelectric_plants_sched406", 7_202),
        ("out_ferc1__yearly_income_statements_sched114", 347_394),
        ("out_ferc1__yearly_operating_expenses_sched320", 618_518),
        ("out_ferc1__yearly_operating_revenues_sched300", 77_646),
        ("out_ferc1__yearly_other_regulatory_liabilities_sched278", 53015),
        ("out_ferc1__yearly_plant_in_service_sched204", 355_918),
        ("out_ferc1__yearly_pumped_storage_plants_sched408", 580),
        ("out_ferc1__yearly_purchased_power_and_exchanges_sched326", 211_794),
        ("out_ferc1__yearly_retained_earnings_sched118", 105_585),
        ("out_ferc1__yearly_sales_by_rate_schedules_sched304", 303_909),
        ("out_ferc1__yearly_small_plants_sched410", 17763),
        ("out_ferc1__yearly_steam_plants_fuel_by_plant_sched402", 26_947),
        ("out_ferc1__yearly_steam_plants_fuel_sched402", 51_238),
        ("out_ferc1__yearly_steam_plants_sched402", 32_975),
        ("out_ferc1__yearly_transmission_lines_sched422", 640_619),
        ("out_ferc1__yearly_utility_plant_summary_sched200", 198_769),
        ("out_ferc1__yearly_small_plants_sched410", 17_763),
    ],
)
def test_minmax_rows(live_dbs, asset_value_loader, expected_rows, asset_key):
    """Verify that output DataFrames don't have too many or too few rows.

    Args:
        live_dbs: Boolean (wether we're using a live or testing DB).
        expected_rows (int): Expected number of rows that the dataframe should
            contain when all data is loaded and is output without aggregation.
        asset_key (str): The name of the asset.
    """
    if not live_dbs:
        pytest.skip("Data validation only works with a live PUDL DB.")
    _ = (
        asset_value_loader.load_asset_value(asset_key)
        .pipe(
            pv.check_min_rows,
            expected_rows=expected_rows,
            margin=0.0,
            df_name=asset_key,
        )
        .pipe(
            pv.check_max_rows,
            expected_rows=expected_rows,
            margin=0.0,
            df_name=asset_key,
        )
    )


@pytest.mark.parametrize(
    "asset_key,unique_subset",
    [
        (
            "_out_ferc1__yearly_plants_utilities",
            ["utility_id_ferc1", "plant_name_ferc1"],
        ),
        (
            "out_ferc1__yearly_steam_plants_fuel_by_plant_sched402",
            ["report_year", "utility_id_ferc1", "plant_name_ferc1"],
        ),
        (
            "out_ferc1__yearly_hydroelectric_plants_sched406",
            [
                "report_year",
                "utility_id_ferc1",
                "plant_name_ferc1",
                "capacity_mw",  # Why does having capacity here make sense???
            ],
        ),
        (
            "out_ferc1__yearly_pumped_storage_plants_sched408",
            [
                "report_year",
                "utility_id_ferc1",
                "plant_name_ferc1",
                "capacity_mw",  # Why does having capacity here make sense???
            ],
        ),
        (
            "out_ferc1__yearly_plant_in_service_sched204",
            ["report_year", "utility_id_ferc1", "ferc_account_label"],
        ),
    ],
)
def test_unique_rows_ferc1(live_dbs, asset_value_loader, asset_key, unique_subset):
    """Test whether dataframe has unique records within a subset of columns."""
    if not live_dbs:
        pytest.skip("Data validation only works with a live PUDL DB.")
    pv.check_unique_rows(
        asset_value_loader.load_asset_value(asset_key),
        subset=unique_subset,
        df_name=asset_key,
    )


@pytest.mark.parametrize(
    "table_name",
    [  # some sample wide guys
        "core_ferc1__yearly_sales_by_rate_schedules_sched304",
        "core_ferc1__yearly_pumped_storage_plants_sched408",
        "core_ferc1__yearly_steam_plants_sched402",
        "core_ferc1__yearly_hydroelectric_plants_sched406",
        "core_ferc1__yearly_transmission_lines_sched422",
        # some sample guys found to have higher filtering diffs
        "core_ferc1__yearly_utility_plant_summary_sched200",
        "core_ferc1__yearly_plant_in_service_sched204",
        "core_ferc1__yearly_operating_expenses_sched320",
        "core_ferc1__yearly_income_statements_sched114",
    ],
)
def test_filter_for_freshest_data(
    ferc1_engine_xbrl, table_name: pd.DataFrame
) -> pd.DataFrame:
    """Test if we are unexpectedly replacing records during filter_for_freshest_data."""

    raw_table_names = TABLE_NAME_MAP_FERC1[table_name]["xbrl"]
    # sometimes there are many raw tables that go into one
    # core table, but usually its a string.
    if isinstance(raw_table_names, str):
        raw_table_names = [raw_table_names]
    xbrls_with_periods = chain.from_iterable(
        (f"raw_ferc1_xbrl__{tn}_instant", f"raw_ferc1_xbrl__{tn}_duration")
        for tn in raw_table_names
    )
    for raw_table_name in xbrls_with_periods:
        logger.info(f"Checking if our filtering methodology works for {raw_table_name}")
        xbrl_table = defs.load_asset_value(raw_table_name)
        if not xbrl_table.empty:
            primary_keys = get_primary_key_raw_xbrl(
                raw_table_name.removeprefix("raw_ferc1_xbrl__"), "ferc1"
            )
            filter_for_freshest_data_xbrl(
                xbrl_table, primary_keys, compare_methods=True
            )
