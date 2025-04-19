"""Classes & functions to process PHMSA natural gas data before loading into the PUDL DB."""

import pandas as pd
from dagster import AssetCheckResult, asset, asset_check

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
        "filing_date",
        "correction_date",
        "data_date",
        "report_number",
        "supplemental_report_number",
        "report_submission_type",
        "original_report",
        "supplementary_report",
        "report_year",
        "log",
        "operator_id_phmsa",
        "operator_name_phmsa",
        "operating_state",
        "operator_type",
        "commodity",
        "office_street_address",
        "office_city",
        "office_state",
        "office_zip",
        "office_county",
        "headquarters_street_address",
        "headquarters_city",
        "headquarters_county",
        "headquarters_state",
        "headquarters_zip",
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
        "form_revision",
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
        "operating_state",
        "headquarters_state",
        "office_state",
        "preparer_email",
        "additional_information",
    ],
}

##############################################################################
# PHMSAGAS transformation logic
##############################################################################


@asset(
    # io_manager_key="pudl_io_manager",
    compute_kind="pandas",
)
def core_phmsagas__yearly_distribution_operators(
    raw_phmsagas__yearly_distribution: pd.DataFrame,
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
    df = raw_phmsagas__yearly_distribution.loc[
        :, YEARLY_DISTRIBUTION_OPERATORS_COLUMNS["columns_to_keep"]
    ].copy()

    # Standardize NAs
    df = standardize_na_values(df)

    # Convert date columns to datetime
    for col in ["report_date", "filing_date", "correction_date", "data_date"]:
        df[col] = pd.to_datetime(df[col])

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
    # We expect these to all be years prior to 2000, and reporting starts in 1970
    mask = (df["report_year"] < 100) & (df["report_year"] >= 70)

    # Convert 2-digit years to appropriate 4-digit format
    df.loc[mask, "report_year"] = 1900 + df.loc[mask, "report_year"]

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

    for state_col in ["headquarters_state", "office_state"]:
        df[state_col] = (
            df[state_col]
            .str.strip()
            .replace(state_to_abbr)
            .where(df[state_col].isin(state_to_abbr.values()), pd.NA)
        )

    # Standardize zip codes
    df["office_zip"] = zero_pad_numeric_string(df["office_zip"], n_digits=5)
    df["headquarters_zip"] = zero_pad_numeric_string(df["headquarters_zip"], n_digits=5)

    # Standardize telephone and fax number format and drop (000)-000-0000
    df = standardize_phone_column(df, ["preparer_phone", "preparer_fax"])

    # Convert percent unaccounted for gas to a fraction
    df["unaccounted_for_gas_fraction"] = df["percent_unaccounted_for_gas"] / 100
    df = df.drop(columns=["percent_unaccounted_for_gas"])

    # Streamline the initial and supplementary report columns
    df["report_submission_type"].mask(
        df["original_report"] == "Y", "Initial", inplace=True
    )
    df["report_submission_type"].mask(
        df["supplementary_report"] == "Y", "Supplemental", inplace=True
    )
    df = df.drop(columns=["original_report", "supplementary_report"])

    # Drop duplicates
    df = df.drop_duplicates()

    # Identify non-unique groups based on our PK
    # There are a small number of records with duplicate report numbers
    # and either duplicate or "test" data in the rows. Remove the 'bad data'
    # to ensure a unique primary key.
    non_unique_groups = df[df.duplicated(["report_number"], keep=False)]

    # Apply some custom filtering logic to non-unique groups
    filtered_non_unique_rows = non_unique_groups.groupby(
        ["report_number"], group_keys=False
    ).apply(combined_filter)

    # There are ten values that we expect to be duplicated.
    assert len(filtered_non_unique_rows) <= 10, (
        f"Found {len(filtered_non_unique_rows)} records with duplicates, expected ten or less."
    )

    # Combine filtered non-unique rows with untouched unique rows
    unique_rows = df.drop(non_unique_groups.index)
    df = pd.concat([unique_rows, filtered_non_unique_rows], ignore_index=True)

    return df


def filter_if_test_in_address(group: pd.DataFrame) -> pd.DataFrame:
    """Filters out rows with "test" in address columns.

    For any group of rows with the same combination of "operator_id_phmsa"
    and "report_number", if at least one row in the group does not contain the string
    "test" (case-insensitive) in either "office_street_address" or
    "headquarters_street_address", keep only the rows in the group that do not contain
    "test" in these columns. If all rows in the group contain "test" in either of the
    columns, leave the group unchanged.

    Args:
        group: A grouped subset of the DataFrame.

    Returns:
        The filtered group of rows.
    """
    # Check if at least one row in the group does NOT contain "test" in both of the specified columns
    contains_test = group.apply(
        lambda row: "test" in str(row["office_street_address"]).lower()
        or "test" in str(row["headquarters_street_address"]).lower(),
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

    Filter to only keep rows where "office_city" value is contained in the
    "operator_name_phmsa" value (case insensitive).

    Args:
        group: A grouped subset of the DataFrame.

    Returns:
        The filtered group of rows.
    """
    # Check if any row has "office_city" contained in "operator_name_phmsa" (case insensitive)
    city_in_name = (
        group["office_city"]
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
        group: A grouped subset of the DataFrame.

    Returns:
        The filtered group of rows.
    """
    # Apply filters
    group = filter_by_city_in_name(group)
    group = filter_if_test_in_address(group)
    return group


@asset_check(asset=core_phmsagas__yearly_distribution_operators, blocking=True)
def _check_unaccounted_for_gas_fraction(df):
    """Check what percentage of unaccounted gas values are reported as a negative number.
    This is technically impossible but allowed by PHMSA.
    """
    # Count the rows where unaccounted gas is negative.
    negative_count = (df["unaccounted_for_gas_fraction"] < 0).sum()

    # Calculate the percentage
    negative_percentage = negative_count / len(df)
    if negative_percentage > 0.15:
        error = f"Percentage of rows with negative unaccounted_for_gas_fraction values: {negative_percentage:.2f}"
        logger.info(error)
        return AssetCheckResult(passed=False, metadata={"errors": error})

    return AssetCheckResult(passed=True)
