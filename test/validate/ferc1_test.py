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
from pudl.transform.ferc1 import _get_primary_key

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
    [
        "core_ferc1__yearly_sales_by_rate_schedules_sched304",
        "core_ferc1__yearly_pumped_storage_plants_sched408",
        "core_ferc1__yearly_steam_plants_sched402",
        "core_ferc1__yearly_hydroelectric_plants_sched406",
        "core_ferc1__yearly_transmission_lines_sched422",
    ],
)
def test_filter_for_freshest_data(
    ferc1_engine_xbrl, table_name: pd.DataFrame
) -> pd.DataFrame:
    """Test if we are unexpectedly replacing records during filter_for_freshest_data.

    NOTE: None of this actually calls any of the code over in pudl.transform.ferc1 rn.
    That's maybe okay because we are mostly testing the data updates mostly here.
    """

    def __apply_diffs(
        duped_groups: pd.core.groupby.DataFrameGroupBy,
    ) -> pd.DataFrame:
        """Take the latest reported non-null value for each group."""
        return duped_groups.last()

    def __best_snapshot(
        duped_groups: pd.core.groupby.DataFrameGroupBy,
    ) -> pd.DataFrame:
        """Take the row that has most non-null values out of each group."""
        # Ignore errors when dropping the "count" column since empty
        # groupby won't have this column.
        return duped_groups.apply(
            lambda df: df.assign(count=df.count(axis="columns"))
            .sort_values(by="count", ascending=True)
            .tail(1)
        ).drop(columns="count", errors="ignore")

    def __compare_dedupe_methodologies(
        apply_diffs: pd.DataFrame, best_snapshot: pd.DataFrame
    ):
        """Compare deduplication methodologies.

        By cross-referencing these we can make sure that the apply-diff
        methodology isn't doing something unexpected.

        The main thing we want to keep tabs on is apply-diff adding new
        non-null values compared to best-snapshot, because some of those
        are instances of a value correctly being reported as `null`.

        Instead of stacking the two datasets, merging by context, and then
        looking for left_only or right_only values, we just count non-null
        values. This is because we would want to use the report_year as a
        merge key, but that isn't available until after we pipe the
        dataframe through `refine_report_year`.
        """
        n_diffs = apply_diffs.count().sum()
        n_best = best_snapshot.count().sum()

        if n_diffs < n_best:
            raise ValueError(
                f"Found {n_diffs} non-null values with apply-diffs"
                f"methodology, and {n_best} with best-snapshot. "
                "apply-diffs should be >= best-snapshot."
            )

        # 2024-04-10: this threshold set by looking at existing values for FERC
        # <=2022. It was updated from .3 to .44 during the 2023 update.
        threshold_ratio = 1.0044
        if (found_ratio := n_diffs / n_best) > threshold_ratio:
            raise ValueError(
                "Found more than expected excess non-null values using the "
                f"currently  implemented apply_diffs methodology (#{n_diffs}) as "
                f"compared to the best_snapshot methodology (#{n_best}). We expected"
                " the apply_diffs methodology to result in no more than "
                f"{threshold_ratio:.2%} non-null records but found {found_ratio:.2%}.\n\n"
                "We are concerned about excess non-null values because apply-diffs "
                "grabs the most recent non-null values. If this error is raised, "
                "investigate filter_for_freshest_data."
            )

    table_name = "core_ferc1__yearly_sales_by_rate_schedules_sched304"
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
            filing_metadata_cols = {"publication_time", "filing_name"}
            primary_key = _get_primary_key(
                raw_table_name.removeprefix("raw_ferc1_xbrl__")
            )
            xbrl_context_cols = [
                c for c in primary_key if c not in filing_metadata_cols
            ]
            original = xbrl_table.sort_values("publication_time")
            dupe_mask = original.duplicated(subset=xbrl_context_cols, keep=False)
            duped_groups = original.loc[dupe_mask].groupby(
                xbrl_context_cols, as_index=False, dropna=True
            )
            apply_diffs = __apply_diffs(duped_groups)
            best_snapshot = __best_snapshot(duped_groups)
            __compare_dedupe_methodologies(
                apply_diffs=apply_diffs, best_snapshot=best_snapshot
            )
