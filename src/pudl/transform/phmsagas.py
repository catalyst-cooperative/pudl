"""Classes & functions to process PHMSA natural gas data before loading into the PUDL DB."""

import pandas as pd
from dagster import AssetIn, asset

import pudl.logging_helpers
from pudl.helpers import (
    fix_na,
    standardize_phone_column,
    standardize_state_columns,
    zero_pad_numeric_string,
)

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
    "columns_with_nas_to_fill": [
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
    "capitalization_exclusion": ["headquarters_address_state", "office_address_state"],
    "cols_for_state_standardization": [
        "headquarters_address_state",
        "office_address_state",
    ],
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
    df = raw_data[YEARLY_DISTRIBUTION_OPERATORS_COLUMNS["columns_to_keep"]]
    df[YEARLY_DISTRIBUTION_OPERATORS_COLUMNS["columns_to_convert_to_ints"]] = df[
        YEARLY_DISTRIBUTION_OPERATORS_COLUMNS["columns_to_convert_to_ints"]
    ].astype("Int64")

    # Ensure all "report_year" values have four digits
    mask = df["report_year"] < 100

    # Convert 2-digit years to appropriate 4-digit format (assume cutoff at year 50)
    # We could also use the first 4 digits of the "report_number" but there was at least one anomaly here with an invalid year
    df.loc[mask, "report_year"] = df.loc[mask, "report_year"].apply(
        lambda x: 2000 + x if x < 50 else 1900 + x
    )

    # Fill NA values with zeroes because these columns are simply counts.
    # Note that "excavation_damage..." columns should sum up to the value in "excavation_damage_total". However, many rows
    # (on the scale of thousands) do not actually sum up to "excavation_damage_total".
    df[YEARLY_DISTRIBUTION_OPERATORS_COLUMNS["columns_with_nas_to_fill"]] = df[
        YEARLY_DISTRIBUTION_OPERATORS_COLUMNS["columns_with_nas_to_fill"]
    ].fillna(0)

    # Fill in bad strings
    df = fix_na(df)

    # Standardize case for city, county, operator name, etc.
    # Capitalize the first letter of each word in all object-type columns except the excluded ones
    df[
        df.select_dtypes(include=["object"]).columns.difference(
            YEARLY_DISTRIBUTION_OPERATORS_COLUMNS["capitalization_exclusion"]
        )
    ] = df[
        df.select_dtypes(include=["object"]).columns.difference(
            YEARLY_DISTRIBUTION_OPERATORS_COLUMNS["capitalization_exclusion"]
        )
    ].apply(lambda col: col.str.title())

    # List of state columns to standardize
    df = standardize_state_columns(
        df, YEARLY_DISTRIBUTION_OPERATORS_COLUMNS["cols_for_state_standardization"]
    )

    # Trim all the object-type columns
    df[df.select_dtypes(include=["object"]).columns] = df.select_dtypes(
        include=["object"]
    ).applymap(lambda x: x.strip() if isinstance(x, str) else x)

    # Standardize telephone and fax number format and drop (000)-000-0000
    df = standardize_phone_column(df, ["preparer_phone", "preparer_fax"])

    # Standardize zip codes
    df["office_address_zip"] = zero_pad_numeric_string(
        df["office_address_zip"], n_digits=5
    )
    df["headquarters_address_zip"] = zero_pad_numeric_string(
        df["headquarters_address_zip"], n_digits=5
    )

    return df
