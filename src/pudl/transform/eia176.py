"""Module to perform data cleaning functions on EIA176 data tables."""

import warnings

import pandas as pd
from dagster import ExperimentalWarning, asset, asset_check

from pudl.logging_helpers import get_logger

logger = get_logger(__name__)

# Asset Checks are still Experimental, silence the warning since we use them
# everywhere.
warnings.filterwarnings("ignore", category=ExperimentalWarning)


@asset
def _core_eia176__data(raw_eia176__data: pd.DataFrame) -> pd.DataFrame:
    """Take entity-attribute-value rows and convert to rows with primary key and one column per variable."""
    raw_eia176__data["variable_name"] = (
        raw_eia176__data["line"] + "_" + raw_eia176__data["atype"]
    )
    primary_key = ["report_year", "area", "id"]

    # TODO should probably sanitize this company name somewhere beforehand
    granular = raw_eia176__data.loc[
        raw_eia176__data.company.str.strip().str.lower() != "total of all companies"
    ]
    unstacked = (
        granular.drop(columns=["itemsort", "item", "atype", "line", "company"])
        .set_index(primary_key + ["variable_name"])
        .unstack(level="variable_name")
    )

    # columns is a weird multi-index with ("value", "actual column name") - clean that up
    unstacked.columns = unstacked.columns.droplevel(0)
    unstacked.columns.name = None  # gets rid of "variable_name" name of columns index

    # TODO instead of "first NA value we see in each column" applied willy-nilly, we could check to see if there are any conflicting non-null values using .count() first.
    wide_table = unstacked.groupby(level=primary_key).first().reset_index()
    return wide_table


@asset_check(asset=_core_eia176__data, blocking=True)
def validate_totals():
    """Compare reported and calculated totals for different geographical aggregates, report any differences."""


def _compare_totals(
    reported_totals: pd.DataFrame,
    calculated_totals: pd.DataFrame,
    groupby_cols: list[str],
) -> pd.DataFrame:
    """Compare two dataframes representing reporting and calculated totals."""
    reset_calculated = (
        calculated_totals.sort_values(by=groupby_cols)
        .reset_index()[groupby_cols + ["value"]]
        .round(2)
    )

    reset_reported = (
        reported_totals.sort_values(by=groupby_cols)
        .reset_index()[groupby_cols + ["value"]]
        .fillna(0)
    )

    return reset_calculated.compare(reset_reported)


# TODO: Reasonable boundaries -- in a script/notebook in the 'validate' directory? How are those executed?
