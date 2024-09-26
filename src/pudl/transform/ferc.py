"""Module for shared helpers for FERC Form transforms."""

import json
from typing import Literal

import pandas as pd

from pudl.workspace.setup import PudlPaths


def apply_diffs(
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


def filter_for_freshest_data_xbrl(
    df: pd.DataFrame, primary_keys, compare_methods: bool = False
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
    if not df.empty:
        filing_metadata_cols = {"publication_time", "filing_name"}
        xbrl_context_cols = [c for c in primary_keys if c not in filing_metadata_cols]
        original = df.sort_values("publication_time")
        dupe_mask = original.duplicated(subset=xbrl_context_cols, keep=False)
        duped_groups = original.loc[dupe_mask].groupby(
            xbrl_context_cols, as_index=False, dropna=True
        )
        never_duped = original.loc[~dupe_mask]
        applied_diffs = apply_diffs(duped_groups)
        if compare_methods:
            best_snapshot = __best_snapshot(duped_groups)
            __compare_dedupe_methodologies(
                apply_diffs=applied_diffs, best_snapshot=best_snapshot
            )

        df = pd.concat([never_duped, applied_diffs], ignore_index=True).drop(
            columns=["publication_time"]
        )
    return df


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
