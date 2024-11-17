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


@pytest.mark.parametrize("table_name", unique_record_tables)
def test_record_id_dupes(pudl_engine, table_name):
    """Verify that the generated ferc1 record_ids are unique."""
    table = pd.read_sql(table_name, pudl_engine)
    n_dupes = table.record_id.duplicated().to_numpy().sum()

    if n_dupes:
        dupe_ids = table.record_id[table.record_id.duplicated()].to_numpy()
        raise AssertionError(
            f"{n_dupes} duplicate record_ids found in {table_name}: {dupe_ids}."
        )


@pytest.mark.parametrize(
    "df_name,cols",
    [
        ("fbp_ferc1", "all"),
        ("fuel_ferc1", "all"),
        ("plant_in_service_ferc1", "all"),
        ("plants_all_ferc1", "all"),
        ("plants_hydro_ferc1", "all"),
        ("plants_pumped_storage_ferc1", "all"),
        ("plants_small_ferc1", "all"),
        ("plants_steam_ferc1", "all"),
        ("pu_ferc1", "all"),
        ("purchased_power_ferc1", "all"),
    ],
)
def test_no_null_cols_ferc1(pudl_out_ferc1, live_dbs, cols, df_name):
    """Verify that output DataFrames have no entirely NULL columns."""
    if not live_dbs:
        pytest.skip("Data validation only works with a live PUDL DB.")
    pv.no_null_cols(
        pudl_out_ferc1.__getattribute__(df_name)(), cols=cols, df_name=df_name
    )


@pytest.mark.parametrize(
    "df_name,expected_rows",
    [
        ("fbp_ferc1", 26_947),
        ("fuel_ferc1", 51_238),
        ("plant_in_service_ferc1", 355_918),
        ("plants_all_ferc1", 58_520),
        ("plants_hydro_ferc1", 7_202),
        ("plants_pumped_storage_ferc1", 580),
        ("plants_small_ferc1", 17_763),
        ("plants_steam_ferc1", 32_975),
        ("pu_ferc1", 7_887),
        ("purchased_power_ferc1", 211_794),
    ],
)
def test_minmax_rows(pudl_out_ferc1, live_dbs, expected_rows, df_name):
    """Verify that output DataFrames don't have too many or too few rows.

    Args:
        pudl_out_ferc1: A PudlTabl output object.
        live_dbs: Boolean (wether we're using a live or testing DB).
        expected_rows (int): Expected number of rows that the dataframe should
            contain when all data is loaded and is output without aggregation.
        df_name (str): Shorthand name identifying the dataframe, corresponding
            to the name of the function used to pull it from the PudlTabl
            output object.
    """
    if not live_dbs:
        pytest.skip("Data validation only works with a live PUDL DB.")
    _ = (
        pudl_out_ferc1.__getattribute__(df_name)()
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
        ("pu_ferc1", ["utility_id_ferc1", "plant_name_ferc1"]),
        ("fbp_ferc1", ["report_year", "utility_id_ferc1", "plant_name_ferc1"]),
        (
            "plants_hydro_ferc1",
            [
                "report_year",
                "utility_id_ferc1",
                "plant_name_ferc1",
                "capacity_mw",  # Why does having capacity here make sense???
            ],
        ),
        (
            "plants_pumped_storage_ferc1",
            [
                "report_year",
                "utility_id_ferc1",
                "plant_name_ferc1",
                "capacity_mw",  # Why does having capacity here make sense???
            ],
        ),
        (
            "plant_in_service_ferc1",
            ["report_year", "utility_id_ferc1", "ferc_account_label"],
        ),
    ],
)
def test_unique_rows_ferc1(pudl_out_ferc1, live_dbs, df_name, unique_subset):
    """Test whether dataframe has unique records within a subset of columns."""
    if not live_dbs:
        pytest.skip("Data validation only works with a live PUDL DB.")
    pv.check_unique_rows(
        pudl_out_ferc1.__getattribute__(df_name)(),
        subset=unique_subset,
        df_name=df_name,
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
