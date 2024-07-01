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
    variable_names = list(raw_eia176__data.groupby("variable_name").count().index)
    wide_table = pd.DataFrame(columns=primary_key + variable_names)

    granular_data = raw_eia176__data[
        (raw_eia176__data["company"] != " Total of All Companies")
    ]
    for report_year, area, id_ in granular_data.groupby(primary_key).count().index:
        # Get the data corresponding to one completed form EIA-176
        form_data = granular_data[
            (granular_data["report_year"] == report_year)
            & (granular_data["area"] == area)
            & (granular_data["id"] == id_)
        ]

        wide_row = {"report_year": report_year, "area": area, "id": id_}

        # Translate each piece of data entered into the form into its own column
        for record in form_data.iterrows():
            form_row = record[1]
            wide_row[form_row["variable_name"]] = form_row["value"]

        wide_table.loc[len(wide_table.index)] = wide_row

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
