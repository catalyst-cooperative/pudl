"""Validate post-ETL FERC Form 1 data and the associated derived outputs.

These tests depend on a FERC Form 1 specific PudlTabl output object, which is a
parameterized fixture that has session scope.
"""
import logging

import pandas as pd
import pytest

from pudl import validate as pv
from pudl.metadata.classes import DataSource

logger = logging.getLogger(__name__)

# These are tables for which individual records have been sliced up and
# turned into columns (except for the transmission table) -- so there's no universally
# unique record ID. But we should parameterize the has_unique_record_ids class
# attributes in the FERC classes.
non_unique_record_id_tables = [
    "core_ferc1__yearly_plant_in_service",
    "core_ferc1__yearly_purchased_power",
    "core_ferc1__yearly_electric_energy_sources",
    "core_ferc1__yearly_electric_energy_dispositions",
    "core_ferc1__yearly_utility_plant_summary",
    "core_ferc1__yearly_transmission_statistics",
    "core_ferc1__yearly_balance_sheet_liabilities",
    "core_ferc1__yearly_balance_sheet_assets",
    "core_ferc1__yearly_income_statement",
    "core_ferc1__yearly_depreciation_amortization_summary",
    "core_ferc1__yearly_electric_plant_depreciation_changes",
    "core_ferc1__yearly_electric_plant_depreciation_functional",
    "core_ferc1__yearly_electric_operating_expenses",
    "core_ferc1__yearly_cash_flow",
    "core_ferc1__yearly_retained_earnings",
    "core_ferc1__yearly_electric_operating_revenues",
    "core_ferc1__yearly_other_regulatory_liabilities",
    "core_ferc1__yearly_electricity_sales_by_rate_schedule",
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
    n_dupes = table.record_id.duplicated().values.sum()

    if n_dupes:
        dupe_ids = table.record_id[table.record_id.duplicated()].values
        raise AssertionError(
            f"{n_dupes} duplicate record_ids found in {table_name}: {dupe_ids}."
        )


@pytest.mark.parametrize(
    "df_name,cols",
    [
        ("fbp_ferc1", "all"),
        ("core_ferc1__yearly_fuel", "all"),
        ("core_ferc1__yearly_plant_in_service", "all"),
        ("plants_all_ferc1", "all"),
        ("core_ferc1__yearly_plants_hydro", "all"),
        ("core_ferc1__yearly_plants_pumped_storage", "all"),
        ("core_ferc1__yearly_plants_small", "all"),
        ("core_ferc1__yearly_plants_steam", "all"),
        ("pu_ferc1", "all"),
        ("core_ferc1__yearly_purchased_power", "all"),
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
        ("fbp_ferc1", 25_421),
        ("core_ferc1__yearly_fuel", 48_841),
        ("core_ferc1__yearly_plant_in_service", 311_986),
        ("plants_all_ferc1", 54_284),
        ("core_ferc1__yearly_plants_hydro", 6_796),
        ("core_ferc1__yearly_plants_pumped_storage", 544),
        ("core_ferc1__yearly_plants_small", 16_235),
        ("core_ferc1__yearly_plants_steam", 30_709),
        ("pu_ferc1", 7_425),
        ("core_ferc1__yearly_purchased_power", 197_523),
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
            "core_ferc1__yearly_plants_hydro",
            [
                "report_year",
                "utility_id_ferc1",
                "plant_name_ferc1",
                "capacity_mw",  # Why does having capacity here make sense???
            ],
        ),
        (
            "core_ferc1__yearly_plants_pumped_storage",
            [
                "report_year",
                "utility_id_ferc1",
                "plant_name_ferc1",
                "capacity_mw",  # Why does having capacity here make sense???
            ],
        ),
        (
            "core_ferc1__yearly_plant_in_service",
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
