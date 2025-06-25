"""Module for shared helpers for FERC Form transforms."""

import json
from typing import Literal

import pandas as pd

import pudl
from pudl.workspace.setup import PudlPaths

logger = pudl.logging_helpers.get_logger(__name__)


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
    applied_diffs: pd.DataFrame,
    best_snapshot: pd.DataFrame,
    xbrl_context_cols: list[str],
):
    """Compare deduplication methodologies.

    By cross-referencing these we can make sure that the apply-diff
    methodology isn't doing something unexpected.

    The main things we want to keep tabs on are: whether apply-diff is
    adding more than expected differences compared to best-snapshot and
    whether or not apply-diff is giving us more values than best-snapshot.
    """

    def _stack_pre_merge(df):
        filing_metadata_cols = {"publication_time", "filing_name"}
        return pd.DataFrame(
            df.set_index(xbrl_context_cols + ["report_year"])
            .drop(columns=filing_metadata_cols)
            .rename_axis("xbrl_factoid", axis=1)
            .stack(0),
            columns=["value"],
        ).reset_index()

    test_filters = pd.merge(
        _stack_pre_merge(applied_diffs),
        _stack_pre_merge(best_snapshot),
        on=xbrl_context_cols + ["report_year", "value", "xbrl_factoid"],
        how="outer",
        indicator=True,
    )
    merge_counts = test_filters._merge.value_counts()
    if (n_diffs := merge_counts.left_only) < (n_best := merge_counts.right_only):
        raise AssertionError(
            "We expected to find more values with the apply_diffs methodology, "
            f"but we found {n_diffs} unique apply_diffs values and {n_best}"
            f"unique best_snapshot values."
        )
    difference_ratio = sum(merge_counts.loc[["left_only", "right_only"]]) / sum(
        merge_counts
    )
    threshold_ratio = 0.025
    if difference_ratio > threshold_ratio:
        raise AssertionError(
            "We expected the currently implemented apply_diffs methodology and the "
            "best_snapshot methodology to result in no more than "
            f"{threshold_ratio:.2%} of records  with differing values but "
            f"found {difference_ratio:.2%}."
        )


def filter_for_freshest_data_xbrl(
    xbrl_table: pd.DataFrame, primary_keys, compare_methods: bool = False
) -> pd.DataFrame:
    """Get most updated values for each XBRL context.

    An XBRL context includes an entity ID, the time period the data applies to, and
    other dimensions such as utility type. Each context has its own ID, but they are
    frequently redefined with the same contents but different IDs - so we identify
    them by their actual content.

    Each row in our SQLite database includes all the facts for one context/filing
    pair.

    If one context is represented in multiple filings, we take the most
    recently-reported non-null value.

    This means that if a utility reports a non-null value, then later
    either reports a null value for it or simply omits it from the report,
    we keep the old non-null value, which may be erroneous. This appears to
    be fairly rare, affecting < 0.005% of reported values.
    """
    if not xbrl_table.empty:
        filing_metadata_cols = {"publication_time", "filing_name"}
        xbrl_context_cols = [c for c in primary_keys if c not in filing_metadata_cols]
        original = xbrl_table.sort_values("publication_time")
        dupe_mask = original.duplicated(subset=xbrl_context_cols, keep=False)
        duped_groups = original.loc[dupe_mask].groupby(
            xbrl_context_cols, as_index=False, dropna=True
        )
        never_duped = original.loc[~dupe_mask]
        applied_diffs = __apply_diffs(duped_groups)
        if compare_methods:
            best_snapshot = __best_snapshot(duped_groups)
            __compare_dedupe_methodologies(
                applied_diffs=applied_diffs,
                best_snapshot=best_snapshot,
                xbrl_context_cols=xbrl_context_cols,
            )

        xbrl_table = pd.concat([never_duped, applied_diffs], ignore_index=True).drop(
            columns=["publication_time"]
        )
    return xbrl_table


def get_primary_key_raw_xbrl(
    sched_table_name: str, ferc_form: Literal["ferc1", "ferc714"]
) -> list[str]:
    """Get the primary key for a raw XBRL table from the XBRL datapackage."""
    # TODO (daz): as of 2023-10-13, our datapackage.json is merely
    # "frictionless-like" so we manually parse it as JSON. once we make our
    # datapackage.json conformant, we will need to at least update the
    # "primary_key" to "primaryKey", but maybe there will be other changes
    # as well.
    with (PudlPaths().output_dir / f"{ferc_form}_xbrl_datapackage.json").open() as f:
        datapackage = json.loads(f.read())
    [table_resource] = [
        tr for tr in datapackage["resources"] if tr["name"] == sched_table_name
    ]
    return table_resource["schema"]["primary_key"]
