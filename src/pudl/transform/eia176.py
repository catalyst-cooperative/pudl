"""Module to perform data cleaning functions on EIA176 data tables."""

import pandas as pd
from dagster import AssetCheckResult, AssetIn, AssetOut, asset_check, multi_asset

from pudl.logging_helpers import get_logger

logger = get_logger(__name__)


@multi_asset(
    outs={
        "core_eia176__yearly_company_data": AssetOut(),
        "core_eia861__yearly_aggregate_data": AssetOut(),
    },
)
def _core_eia176__data(
    raw_eia176__data: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Take raw list and return two wide tables with primary keys and one column per variable.

    One table with data for each year and company, one with state- and US-level aggregates per year.
    """
    raw_eia176__data = raw_eia176__data.astype({"report_year": int, "value": float})
    raw_eia176__data["variable_name"] = (
        raw_eia176__data["line"] + "_" + raw_eia176__data["atype"]
    )

    aggregate_primary_key = ["report_year", "area"]
    company_primary_key = aggregate_primary_key + ["id"]
    company_drop_columns = ["itemsort", "item", "atype", "line", "company"]
    # We must drop 'id' here and cannot use as primary key because its arbitrary/duplicate in aggregate records
    # 'id' is a reliable ID only in the context of granular company data
    aggregate_drop_columns = company_drop_columns + ["id"]

    long_company = raw_eia176__data.loc[
        raw_eia176__data.company != "total of all companies"
    ]
    wide_company = get_wide_table(
        long_table=long_company.drop(columns=company_drop_columns),
        primary_key=company_primary_key,
    )

    long_aggregate = raw_eia176__data.loc[
        raw_eia176__data.company == "total of all companies"
    ]
    wide_aggregate = get_wide_table(
        long_table=long_aggregate.drop(columns=aggregate_drop_columns),
        primary_key=aggregate_primary_key,
    )

    return wide_company, wide_aggregate


def get_wide_table(long_table: pd.DataFrame, primary_key: list[str]) -> pd.DataFrame:
    """Take a 'long' or entity-attribute-value table and return a wide table with one column per attribute/variable."""
    unstacked = long_table.set_index(primary_key + ["variable_name"]).unstack(
        level="variable_name"
    )
    unstacked.columns = unstacked.columns.droplevel(0)
    unstacked.columns.name = None  # gets rid of "variable_name" name of columns index
    return unstacked.reset_index().fillna(0)


@asset_check(
    asset="core_eia176__yearly_company_data",
    additional_ins={"core_eia861__yearly_aggregate_data": AssetIn()},
    blocking=True,
)
def validate_totals(
    core_eia176__yearly_company_data: pd.DataFrame,
    core_eia861__yearly_aggregate_data: pd.DataFrame,
) -> AssetCheckResult:
    """Compare reported and calculated totals for different geographical aggregates, report any differences."""
    # First make it so we can directly compare reported aggregates to groupings of granular data
    comparable_aggregates = core_eia861__yearly_aggregate_data.sort_values(
        ["report_year", "area"]
    ).fillna(0)

    # Group company data into state-level data and compare to reported totals
    state_data = (
        core_eia176__yearly_company_data.drop(columns="id")
        .groupby(["report_year", "area"])
        .sum()
        .reset_index()
    )
    aggregate_state = comparable_aggregates[
        comparable_aggregates.area != "u.s. total"
    ].reset_index(drop=True)
    # Compare using the same columns
    state_diff = aggregate_state[state_data.columns].compare(state_data)

    # Group calculated state-level data into US-level data and compare to reported totals
    us_data = (
        state_data.drop(columns="area")
        .groupby("report_year")
        .sum()
        .sort_values("report_year")
        .reset_index()
    )
    aggregate_us = (
        comparable_aggregates[comparable_aggregates.area == "u.s. total"]
        .drop(columns="area")
        .sort_values("report_year")
        .reset_index(drop=True)
    )
    # Compare using the same columns
    us_diff = aggregate_us[us_data.columns].compare(us_data)

    return AssetCheckResult(passed=bool(us_diff.empty and state_diff.empty))


# TODO: Reasonable boundaries -- in a script/notebook in the 'validate' directory? How are those executed?
