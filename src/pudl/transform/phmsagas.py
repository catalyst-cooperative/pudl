"""Classes & functions to process PHMSA natural gas data before loading into the PUDL DB."""

import pandas as pd
from dagster import AssetIn, asset

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
        "report_number",  # not in pudl/metadata/fields.py
        "report_submission_type",  # not in pudl/metadata/fields.py
        "report_year",
        # None of the columns below are in pudl/metadata/fields.py
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
    ],
    "capitalization_exclusion": ["headquarters_address_state", "office_address_state"],
}

##############################################################################
# PHMSAGAS transformation logic
##############################################################################


@asset(
    ins={"raw_data": AssetIn(key="raw_phmsagas__yearly_distribution")},
    io_manager_key=None,  # TODO: check on this one ("pudl_io_manager"? something else?)
    compute_kind=None,  # TODO: check on this one
)
def core_phmsagas__yearly_distribution_operators(
    raw_data: pd.DataFrame,
) -> pd.DataFrame:
    """Build the :ref:`core_phmsagas__yearly_distribution_operators`."""
    df = raw_data.loc[
        :, YEARLY_DISTRIBUTION_OPERATORS_COLUMNS["columns_to_keep"]
    ].copy()

    # Standardize NAs
    df = standardize_na_values(df)

    # Initial string cleaning
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].str.strip()

    # Specify the columns to convert to integer type
    cols_to_convert = YEARLY_DISTRIBUTION_OPERATORS_COLUMNS[
        "columns_to_convert_to_ints"
    ]

    # Fill NaN values with pd.NA, then cast to "Int64" nullable integer type
    df[cols_to_convert] = df[cols_to_convert].fillna(pd.NA).astype("Int64")

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

    return df
