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
from pudl.metadata.enums import (
    DAMAGE_SUB_TYPES_PHMSAGAS,
    DAMAGE_TYPES_PHMSAGAS,
    INSTALL_DECADE_PATTERN_PHMSAGAS,
    LEAK_SOURCE_PHMSAGAS,
    MAIN_PIPE_SIZES_PHMSAGAS,
    MATERIAL_TYPES_PHMSAGAS,
)

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
        "office_city_fips",
        "office_county_fips",
        "office_state_fips",
        "operating_state_fips",
        "headquarters_street_address",
        "headquarters_city",
        "headquarters_county",
        "headquarters_state",
        "headquarters_zip",
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
        "data_maturity",
        # These are numeric columns that didn't fit into the melted
        # numeric tables.
        "all_known_leaks_scheduled_for_repair",
        "all_known_leaks_scheduled_for_repair_main",
        "average_service_length_feet",
        "hazardous_leaks_mechanical_joint_failure",
        "main_other_material_detail",
    ],
    "columns_to_convert_to_ints": [
        "report_year",
        "report_number",
        "operator_id_phmsa",
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

YEARLY_DISTRIBUTION_IDX_ISH = [
    "report_year",
    "report_number",
    "operator_id_phmsa",
    "commodity",
    "operating_state",
]


MELT_PATTERNS = {
    "_core_phmsagas__yearly_distribution_by_material": {
        "main_pattern": rf"^main_({'|'.join(MATERIAL_TYPES_PHMSAGAS)})_miles$",
        "services_pattern": rf"^services_({'|'.join(MATERIAL_TYPES_PHMSAGAS)})$",
    },
    "_core_phmsagas__yearly_distribution_by_install_decade": {
        "main_pattern": "main_" + INSTALL_DECADE_PATTERN_PHMSAGAS + "_miles",
        "services_pattern": "services_" + INSTALL_DECADE_PATTERN_PHMSAGAS,
    },
    "_core_phmsagas__yearly_distribution_leaks": {
        "main_pattern": rf"^(all_leaks|hazardous_leaks)_({'|'.join(LEAK_SOURCE_PHMSAGAS)})_mains$",
        "services_pattern": rf"^(all_leaks|hazardous_leaks)_({'|'.join(LEAK_SOURCE_PHMSAGAS)})_services$",
    },
    "_core_phmsagas__yearly_distribution_by_material_and_size": {
        "main_pattern": rf"^main_({'|'.join(MATERIAL_TYPES_PHMSAGAS)})_({'|'.join(MAIN_PIPE_SIZES_PHMSAGAS)})_miles$",
        "services_pattern": rf"^services_({'|'.join(MATERIAL_TYPES_PHMSAGAS)})_(.*)$",
    },
    "_core_phmsagas__yearly_distribution_excavation_damages": {
        "damage_pattern": rf"^excavation_damage_(?:{'|'.join(DAMAGE_TYPES_PHMSAGAS)})_({'|'.join(DAMAGE_SUB_TYPES_PHMSAGAS)})$"
    },
}


@asset_check(asset="raw_phmsagas__yearly_distribution", blocking=True)
def _check_all_raw_columns_being_transformed(raw_df: pd.DataFrame):
    """Check to ensure that we are transforming all of the raw columns.

    Because we are using a lot of regex patterns to identify the columns
    to transform into various tables, we run this check to make sure the
    we are actually finding all of the raw columns. If a column is flagged
    here, check the column mapping and the patterns in MELT_PATTERNS.1
    """
    pattern_cols = []
    for patterns in MELT_PATTERNS.values():
        for pattern in patterns.values():
            pattern_cols = pattern_cols + (list(raw_df.filter(regex=pattern)))
    transformed_cols = set(
        YEARLY_DISTRIBUTION_OPERATORS_COLUMNS["columns_to_keep"] + pattern_cols
    )
    untransformed_columns = set(raw_df.columns).difference(transformed_cols)
    if untransformed_columns:
        raise AssertionError(
            f"Found {len(untransformed_columns)} columns that are not incorporated into the transform, but expected none: {untransformed_columns}"
        )


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
    df["report_submission_type"] = (
        df["report_submission_type"]
        .mask(df["original_report"] == "Y", "Initial")
        .mask(df["supplementary_report"] == "Y", "Supplemental")
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


def _dedupe_year_distribution_idx(
    raw_phmsagas__yearly_distribution: pd.DataFrame,
) -> pd.DataFrame:
    """Remove the rare duplicates in the expected primary key.

    There are 49 found records which have duplicate values for the expected
    primary key of this table. Many manipulations are much easier when we have a
    unique primary key - merges for instance! So we want to remove these duplicates.

    On visual inspection, these duplicates either look mostly the same or have one
    record with most or all of the non-null or non-zero values. Therefore, we sort
    the total columns and then drop duplicates so we kept the records which have the
    most information. This is not the most robust method of de-duplicating but there
    are so few records compared to the >90k total records.

    """
    tot_cols = list(raw_phmsagas__yearly_distribution.filter(like="_total").columns)
    raw_phmsagas__yearly_distribution = raw_phmsagas__yearly_distribution.sort_values(
        by=tot_cols, ascending=False
    )
    dupe_mask = raw_phmsagas__yearly_distribution.duplicated(
        subset=YEARLY_DISTRIBUTION_IDX_ISH, keep=False
    )
    assert len(raw_phmsagas__yearly_distribution[dupe_mask]) < 50
    return pd.concat(
        [
            raw_phmsagas__yearly_distribution[~dupe_mask],
            raw_phmsagas__yearly_distribution[dupe_mask].drop_duplicates(
                subset=YEARLY_DISTRIBUTION_IDX_ISH, keep="first"
            ),
        ],
    )


def _assign_cols_from_patterns(
    df: pd.DataFrame, col_patterns: dict, pattern_col: str
) -> pd.DataFrame:
    """Add new columns based on regex patterns within an existing column.

    Args:
        df: the dataframe with ``pattern_col``
        col_patterns: dictionary with new column name (keys) and the regex
            pattern found within ``pattern_col`` to extract into the new
            column (values)
        pattern_col: name of column to extract the patterns from.
    """
    for col_name, pattern in col_patterns.items():
        df[col_name] = df[pattern_col].str.extract(pattern)
    return df


def _melt_merge_main_services(
    raw_phmsagas__yearly_distribution: pd.DataFrame,
    main_pattern: str,
    services_pattern: str,
    col_patterns: dict[str, str],
) -> pd.DataFrame:
    """Filter, melt, add columns then merge miles of main and service."""
    deduped_raw = _dedupe_year_distribution_idx(raw_phmsagas__yearly_distribution)
    logger.info("Melting main")
    main = (
        deduped_raw.set_index(YEARLY_DISTRIBUTION_IDX_ISH)
        .filter(regex=main_pattern)
        .dropna(how="all", axis="index")
        .convert_dtypes()
        .melt(ignore_index=False, var_name="melt_col", value_name="mains_miles")
        .pipe(_assign_cols_from_patterns, col_patterns, "melt_col")
        .drop(columns=["melt_col"])
        .set_index(list(col_patterns.keys()), append=True)
        .dropna(how="all", axis="index")
    )
    logger.info("Melting service")
    services = (
        deduped_raw.set_index(YEARLY_DISTRIBUTION_IDX_ISH)
        .filter(regex=services_pattern)
        .dropna(how="all", axis="index")
        .convert_dtypes()
        .melt(ignore_index=False, var_name="melt_col", value_name="services")
        .pipe(_assign_cols_from_patterns, col_patterns, "melt_col")
        .drop(columns=["melt_col"])
        .set_index(list(col_patterns.keys()), append=True)
        .dropna(how="all", axis="index")
    )
    return pd.merge(
        main,
        services,
        left_index=True,
        right_index=True,
        how="outer",
        validate="1:1",
        sort=False,
    ).reset_index()


@asset
def _core_phmsagas__yearly_distribution_by_material(
    raw_phmsagas__yearly_distribution: pd.DataFrame,
) -> pd.DataFrame:
    """Transform the _core table of the miles of main and services by material."""
    return _melt_merge_main_services(
        raw_phmsagas__yearly_distribution,
        MELT_PATTERNS["_core_phmsagas__yearly_distribution_by_material"][
            "main_pattern"
        ],
        MELT_PATTERNS["_core_phmsagas__yearly_distribution_by_material"][
            "services_pattern"
        ],
        {"material": rf"({'|'.join(MATERIAL_TYPES_PHMSAGAS)})"},
    )


@asset
def _core_phmsagas__yearly_distribution_by_install_decade(
    raw_phmsagas__yearly_distribution: pd.DataFrame,
) -> pd.DataFrame:
    """Transform the _core table of the miles of main and services by decade."""
    return _melt_merge_main_services(
        raw_phmsagas__yearly_distribution,
        MELT_PATTERNS["_core_phmsagas__yearly_distribution_by_install_decade"][
            "main_pattern"
        ],
        MELT_PATTERNS["_core_phmsagas__yearly_distribution_by_install_decade"][
            "services_pattern"
        ],
        {"install_decade": INSTALL_DECADE_PATTERN_PHMSAGAS},
    )


@asset
def _core_phmsagas__yearly_distribution_by_material_and_size(
    raw_phmsagas__yearly_distribution: pd.DataFrame,
) -> pd.DataFrame:
    """Transform the _core table of the miles of main and services by material type and size.

    This table represents the bulk of the wide raw columns, which means it ends up being
    nearly 8 million records. This transform includes the standard
    :ref:`_melt_merge_main_services` as well as adding in a column describing the "other"
    material type (``main_other_material_detail``).
    """
    df = _melt_merge_main_services(
        raw_phmsagas__yearly_distribution,
        MELT_PATTERNS["_core_phmsagas__yearly_distribution_by_material_and_size"][
            "main_pattern"
        ],
        MELT_PATTERNS["_core_phmsagas__yearly_distribution_by_material_and_size"][
            "services_pattern"
        ],
        {
            "main_size": rf"({'|'.join(MAIN_PIPE_SIZES_PHMSAGAS)})",
            "material": rf"({'|'.join(MATERIAL_TYPES_PHMSAGAS)})",
        },
    )
    # Add in the other_material_detail column
    other_material_detail = raw_phmsagas__yearly_distribution.assign(material="other")[
        YEARLY_DISTRIBUTION_IDX_ISH + ["material", "main_other_material_detail"]
    ].dropna(subset=["main_other_material_detail"])
    return df.merge(
        other_material_detail,
        on=YEARLY_DISTRIBUTION_IDX_ISH + ["material"],
        how="outer",
        validate="m:1",
    )


@asset
def _core_phmsagas__yearly_distribution_leaks(
    raw_phmsagas__yearly_distribution: pd.DataFrame,
) -> pd.DataFrame:
    """Transform table of leaks - broken out by source and leak severity."""
    return _melt_merge_main_services(
        raw_phmsagas__yearly_distribution,
        MELT_PATTERNS["_core_phmsagas__yearly_distribution_leaks"]["main_pattern"],
        MELT_PATTERNS["_core_phmsagas__yearly_distribution_leaks"]["services_pattern"],
        {
            "leak_severity": r"^(all_leaks|hazardous_leaks)",
            "leak_source": r"^(?:all_leaks|hazardous_leaks)_(.*)_(?:mains|services)",
        },
    )


@asset
def _core_phmsagas__yearly_distribution_excavation_damages(
    raw_phmsagas__yearly_distribution: pd.DataFrame,
) -> pd.DataFrame:
    """Transform table of damages - broken out by type and sub-type."""
    damage_pattern = MELT_PATTERNS[
        "_core_phmsagas__yearly_distribution_excavation_damages"
    ]["damage_pattern"]
    col_patterns = {
        "damage_type": rf"({'|'.join(DAMAGE_TYPES_PHMSAGAS)})",
        "damage_sub_type": damage_pattern,
    }
    return (
        _dedupe_year_distribution_idx(raw_phmsagas__yearly_distribution)
        .set_index(YEARLY_DISTRIBUTION_IDX_ISH)
        .filter(regex=damage_pattern)
        .dropna(how="all", axis="index")
        .convert_dtypes()
        # TODO: what are damages? are they $$?
        .melt(ignore_index=False, var_name="melt_col", value_name="damages")
        .pipe(_assign_cols_from_patterns, col_patterns, "melt_col")
        .drop(columns=["melt_col"])
        .set_index(list(col_patterns.keys()), append=True)
        .dropna(how="all", axis="index")
    )
