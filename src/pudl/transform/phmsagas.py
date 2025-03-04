"""Classes & functions to process PHMSA natural gas data before loading into the PUDL DB."""

import pandas as pd
from dagster import AssetCheckResult, AssetIn, asset, asset_check

import pudl.logging_helpers
from pudl.helpers import (
    standardize_na_values,
    standardize_phone_column,
    zero_pad_numeric_string,
)
from pudl.metadata.dfs import POLITICAL_SUBDIVISIONS

logger = pudl.logging_helpers.get_logger(__name__)

##############################################################################
# Constants required for transforming PHMSAGAS
##############################################################################

YEARLY_DISTRIBUTION_OPERATORS_COLUMNS = {
    "columns_to_keep": [
        "report_date",
        "report_number",
        "report_submission_type",
        "report_year",
        "operator_id_phmsa",
        "operator_name_phmsa",
        "office_address_street",
        "office_address_city",
        "office_address_state",
        "office_address_zip",
        "office_address_county",
        "headquarters_address_street",
        "headquarters_address_city",
        "headquarters_address_state",
        "headquarters_address_zip",
        "headquarters_address_county",
        "excavation_damage_excavation_practices",
        "excavation_damage_locating_practices",
        "excavation_damage_one_call_notification",
        "excavation_damage_other",
        "excavation_damage_total",
        "excavation_tickets",
        "services_efv_in_system",
        "services_efv_installed",
        "services_shutoff_valve_in_system",
        "services_shutoff_valve_installed",
        "federal_land_leaks_repaired_or_scheduled",
        "percent_unaccounted_for_gas",
        "additional_information",
        "preparer_email",
        "preparer_fax",
        "preparer_name",
        "preparer_phone",
        "preparer_title",
    ],
    "columns_to_convert_to_ints": [
        "report_year",
        "report_number",
        "operator_id_phmsa",
        "excavation_damage_excavation_practices",
        "excavation_damage_locating_practices",
        "excavation_damage_one_call_notification",
        "excavation_damage_other",
        "excavation_damage_total",
        "excavation_tickets",
        "services_efv_in_system",
        "services_efv_installed",
        "services_shutoff_valve_in_system",
        "services_shutoff_valve_installed",
        "federal_land_leaks_repaired_or_scheduled",
    ],
    "capitalization_exclusion": [
        "headquarters_address_state",
        "office_address_state",
        "preparer_email",
        "additional_information",
    ],
}

##############################################################################
# PHMSAGAS transformation logic
##############################################################################


@asset(
    ins={"raw_data": AssetIn(key="raw_phmsagas__yearly_distribution")},
    io_manager_key="pudl_io_manager",
    compute_kind="pandas",
)
def core_phmsagas__yearly_distribution_operators(
    raw_data: pd.DataFrame,
) -> pd.DataFrame:
    """Pull and transform the yearly distribution PHMSA data into operator-level data.

    Transformations include:

    * Standardize NAs.
    * Strip blank spaces around string values.
    * Convert specific columns to integers.
    * Standardize report_year values.
    * Standardize address columns.
    * Standardize phone and fax numbers.

    Args:
        raw_phmsagas__yearly_distribution: The raw ``raw_phmsagas__yearly_distribution`` dataframe.

    Returns:
        Transformed ``core_phmsagas__yearly_distribution_operators`` dataframe.

    """
    df = raw_data.loc[
        :, YEARLY_DISTRIBUTION_OPERATORS_COLUMNS["columns_to_keep"]
    ].copy()

    # Standardize NAs
    df = standardize_na_values(df)

    # Convert report date to datetime
    df["report_date"] = pd.to_datetime(df["report_date"])

    # Initial string cleaning
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].str.strip()

    # Specify the columns to convert to integer type
    cols_to_convert = YEARLY_DISTRIBUTION_OPERATORS_COLUMNS[
        "columns_to_convert_to_ints"
    ]

    # Use convert_dtypes() to convert columns to the most appropriate nullable types
    df[cols_to_convert] = df[cols_to_convert].convert_dtypes()

    # Ensure all "report_year" values have four digits
    mask = df["report_year"] < 100

    # Convert 2-digit years to appropriate 4-digit format (assume cutoff at year 50)
    # We could also use the first 4 digits of the "report_number" but there was at least one anomaly here with an invalid year
    df.loc[mask, "report_year"] = 2000 + df.loc[mask, "report_year"].where(
        df.loc[mask, "report_year"] < 50, 1900
    )

    # Standardize case for city, county, operator name, etc.
    # Capitalize the first letter of each word in a list of columns
    cap_cols = df.select_dtypes(include=["object"]).columns.difference(
        YEARLY_DISTRIBUTION_OPERATORS_COLUMNS["capitalization_exclusion"]
    )
    for col in cap_cols:
        df[col] = df[col].str.title()

    # Standardize state abbreviations
    state_to_abbr = {
        x.subdivision_name: x.subdivision_code
        for x in POLITICAL_SUBDIVISIONS.itertuples()
        if x.country_code == "USA" and x.subdivision_type == "state"
    }
    state_to_abbr.update(
        {
            x.subdivision_code: x.subdivision_code
            for x in POLITICAL_SUBDIVISIONS.itertuples()
            if x.country_code == "USA" and x.subdivision_type == "state"
        }
    )

    for state_col in ["headquarters_address_state", "office_address_state"]:
        df[state_col] = (
            df[state_col]
            .str.strip()
            .replace(state_to_abbr)
            .where(df[state_col].isin(state_to_abbr.values()), pd.NA)
        )

    # Standardize zip codes
    df["office_address_zip"] = zero_pad_numeric_string(
        df["office_address_zip"], n_digits=5
    )
    df["headquarters_address_zip"] = zero_pad_numeric_string(
        df["headquarters_address_zip"], n_digits=5
    )

    # Standardize telephone and fax number format and drop (000)-000-0000
    df = standardize_phone_column(df, ["preparer_phone", "preparer_fax"])

    # Drop duplicates
    df = df.drop_duplicates()

    # Identify non-unique groups based on our PKs
    non_unique_groups = df[
        df.groupby(["operator_id_phmsa", "report_number"])["report_number"].transform(
            "size"
        )
        > 1
    ]

    # Apply some custom filtering logic to non-unique groups
    filtered_non_unique_rows = non_unique_groups.groupby(
        ["operator_id_phmsa", "report_number"], group_keys=False
    ).apply(combined_filter)

    # Combine filtered non-unique rows with untouched unique rows
    unique_rows = df.drop(non_unique_groups.index)
    df = pd.concat([unique_rows, filtered_non_unique_rows], ignore_index=True)

    return df


def filter_if_test_in_address(group: pd.DataFrame) -> pd.DataFrame:
    """Filters out rows with "test" in address columns.

    For any group of rows with the same combination of "operator_id_phmsa"
    and "report_number", if at least one row in the group does not contain the string
    "test" (case-insensitive) in either "office_address_street" or
    "headquarters_address_street", keep only the rows in the group that do not contain
    "test" in these columns. If all rows in the group contain "test" in either of the
    columns, leave the group unchanged.

    Args:
        group (DataFrame): A grouped subset of the DataFrame.

    Returns:
        DataFrame: The filtered group of rows.
    """
    # Check if at least one row in the group does NOT contain "test" in both of the specified columns
    contains_test = group.apply(
        lambda row: "test" in str(row["office_address_street"]).lower()
        or "test" in str(row["headquarters_address_street"]).lower(),
        axis=1,
    )
    has_non_test = not contains_test.all()

    if has_non_test:
        # Keep rows where "test" does NOT appear in either column
        return group[~contains_test]
    # If all rows have "test", keep the group as is
    return group


def filter_by_city_in_name(group: pd.DataFrame) -> pd.DataFrame:
    """Deduplication to keep duplicated rows where city and operator name overlap.

    Filter to only keep rows where "office_address_city" value is contained in the
    "operator_name_phmsa" value (case insensitive).

    Args:
        group (pd.DataFrame): A grouped subset of the DataFrame.

    Returns:
        pd.DataFrame: The filtered group of rows.
    """
    # Check if any row has "office_address_city" contained in "operator_name_phmsa" (case insensitive)
    city_in_name = (
        group["office_address_city"]
        .str.lower()
        .apply(
            lambda city: any(
                city in name.lower() for name in group["operator_name_phmsa"]
            )
        )
    )

    if city_in_name.any():
        # If any city is contained in the operator name, keep only those rows
        return group[city_in_name]
    # If no city is contained in the operator name, return the group as-is
    return group


def combined_filter(group: pd.DataFrame) -> pd.DataFrame:
    """Apply all required filters to DataFrame.

    Args:
        group (pd.DataFrame): A grouped subset of the DataFrame.

    Returns:
        pd.DataFrame: The filtered group of rows.
    """
    # Apply filters
    group = filter_by_city_in_name(group)
    group = filter_if_test_in_address(group)
    return group


@asset_check(asset=core_phmsagas__yearly_distribution_operators, blocking=True)
def _check_percent_unaccounted_for_gas(df):
    # Count the rows where percent_unaccounted_for_gas is negative
    negative_count = (df["percent_unaccounted_for_gas"] < 0).sum()

    # Calculate the percentage
    negative_percentage = negative_count / len(df)
    if negative_percentage > 0.15:
        error = f"Percentage of rows with negative percent_unaccounted_for_gas values: {negative_percentage:.2f}"
        logger.info(error)
        return AssetCheckResult(passed=False, metadata={"errors": error})

    return AssetCheckResult(passed=True)


@asset_check(asset=core_phmsagas__yearly_distribution_operators, blocking=True)
def _check_pk_deduplication(df):
    """Check if the size of filtered non-unique rows exceeds the threshold."""
    # Identify non-unique groups
    non_unique_groups = df.groupby(["operator_id_phmsa", "report_number"]).filter(
        lambda group: len(group) > 1
    )

    # Apply the filters to non-unique groups
    filtered_non_unique_rows = non_unique_groups.groupby(
        ["operator_id_phmsa", "report_number"], group_keys=False
    ).apply(combined_filter)

    if len(filtered_non_unique_rows) > 10:
        error = (
            f"Number of filtered non-unique rows ({len(filtered_non_unique_rows)})\n"
            f"Exceeds the threshold of 10."
        )
        logger.info(error)
        return AssetCheckResult(passed=False, metadata={"errors": error})

    return AssetCheckResult(passed=True)
