"""Pipeline tests for the PUDL Diff tool, run against the fast ETL's own outputs.

Compares tables in ``$PUDL_OUTPUT/parquet`` (as built by the ``prebuilt_outputs``
fixture) against themselves, and compares a handful of ``core_*``/``out_*`` table
pairs known to be purely additive - the ``out_`` table adds columns on top of the
``core_`` table without dropping rows or modifying shared column values.
"""

import pytest
from pudl_diff.dataset import PudlDiffDataset
from pudl_diff.rows import KeyedRowDiff
from pudl_diff.table import run_table_diff

from pudl.workspace.setup import PudlPaths

#: Tables holding between 10,000 and 1,000,000 rows in a full ETL run (fewer in the
#: fast ETL these tests actually run against), mixing tables with and without a
#: primary key.
SELF_COMPARISON_TABLES = [
    "core_eia860__scd_generators",  # has a primary key
    "core_eia860__scd_boilers",  # has a primary key
    "core_eia923__fuel_receipts_costs",  # no primary key
    "core_epa__assn_eia_epacamd",  # no primary key
]

#: core_*/out_* pairs verified (by reading pudl.output.eia) to be purely additive:
#: each out_ table left-joins the core_ table against entity/assn tables keyed
#: uniquely on the join columns, with no dropna/fillna touching the core table's
#: own columns. Not every core_*/out_* pair has this property - several out_ tables
#: backfill or drop rows relative to their core_ table - so this list is deliberately
#: short rather than exhaustive.
CORE_OUT_ADDITIVE_PAIRS = [
    ("core_eia860__scd_utilities", "out_eia__yearly_utilities"),
    ("core_eia860__scd_boilers", "out_eia__yearly_boilers"),
]


@pytest.fixture
def parquet_dataset(prebuilt_outputs, pudl_test_paths: PudlPaths) -> PudlDiffDataset:
    """A :class:`~pudl_diff.dataset.PudlDiffDataset` over the prebuilt ETL outputs."""
    return PudlDiffDataset(pudl_test_paths.parquet_path())


@pytest.mark.parametrize("table_name", SELF_COMPARISON_TABLES)
def test_table_is_identical_to_itself(
    table_name: str, parquet_dataset: PudlDiffDataset
):
    """A table diffed against itself should always come back identical."""
    run = run_table_diff(parquet_dataset, parquet_dataset, table_name)
    assert run.success, run.error
    assert run.result is not None
    assert run.result.is_identical, (
        f"{table_name} was not found identical to itself: "
        f"schema_diff={run.result.schema_diff}, "
        f"row_count_diff={run.result.row_count_diff}, "
        f"row_diff_skipped_reason={run.result.row_diff_skipped_reason}"
    )


@pytest.mark.parametrize(("core_table", "out_table"), CORE_OUT_ADDITIVE_PAIRS)
def test_core_out_table_is_purely_additive(
    core_table: str, out_table: str, parquet_dataset: PudlDiffDataset
):
    """Check that these out_ tables only add columns on top of their core_ table.

    Assertions use :func:`~pudl_diff.table.run_table_diff`'s result fields.
    """
    run = run_table_diff(
        parquet_dataset, parquet_dataset, core_table, right_table_name=out_table
    )
    assert run.success, run.error
    assert run.result is not None
    result = run.result

    assert not result.schema_diff.columns_only_in_left, (
        f"{core_table} has columns not present in {out_table}: "
        f"{result.schema_diff.columns_only_in_left}"
    )
    assert not result.schema_diff.dtype_changes, (
        f"{core_table} and {out_table} disagree on dtypes for shared columns: "
        f"{result.schema_diff.dtype_changes}"
    )
    assert result.row_count_diff.is_identical, (
        f"{core_table} has {result.row_count_diff.left_row_count} rows but "
        f"{out_table} has {result.row_count_diff.right_row_count}"
    )

    assert result.row_diff_skipped_reason is None, (
        f"Row-level comparison of {core_table} vs {out_table} was skipped: "
        f"{result.row_diff_skipped_reason}"
    )
    assert isinstance(result.row_diff, KeyedRowDiff)
    assert result.row_diff.pk_diff.is_identical, (
        f"{core_table} and {out_table} don't share the same set of primary keys: "
        f"{result.row_diff.pk_diff.only_in_left_count} only in {core_table}, "
        f"{result.row_diff.pk_diff.only_in_right_count} only in {out_table}"
    )
    assert not result.row_diff.column_changes, (
        f"{out_table} modifies shared non-primary-key values from {core_table}: "
        f"{result.row_diff.column_changes}"
    )
