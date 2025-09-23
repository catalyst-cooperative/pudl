"""Classes & functions to process PHMSA natural gas data before loading into the PUDL DB."""

import numpy as np
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
YEARLY_DISTRIBUTION_FILING_COLUMNS = [
    "report_number",  # PK hopefully
    "log_number",  # we drop this after a check
    "operator_id_phmsa",
    "report_year",
    "commodity",  # these two need to be in here for clean_raw_phmsagas__yearly_distribution
    "operating_state",
    "filing_date",
    "initial_filing_date",
    "filing_correction_date",
    "report_filing_type",
    "data_date",
    "is_original_filing",  # TODO: convert to bool
    "is_correction_filing",  # TODO: convert to bool
    "form_revision_id",
    "preparer_name",
    "preparer_title",
    "preparer_phone",
    "preparer_fax",
    "preparer_email",
]

YEARLY_DISTRIBUTION_OPERATORS_COLUMNS = {
    "columns_to_keep": [
        "filing_date",
        "data_date",
        "report_number",
        "supplemental_report_number",
        "report_year",
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
        "additional_information",
        "data_maturity",
    ],
    "columns_to_convert_to_ints": [
        "report_year",
        "report_number",
        "operator_id_phmsa",
    ],
    "capitalization_exclusion": [
        "operating_state",
        "headquarters_state",
        "office_state",
        "preparer_email",
        "additional_information",
    ],
}

YEARLY_DISTRIBUTION_MISC_COLUMNS = [
    # These are numeric columns that didn't fit into the melted
    # numeric tables.
    "all_known_leaks_scheduled_for_repair_main",
    "all_known_leaks_scheduled_for_repair",
    "hazardous_leaks_mechanical_joint_failure",
    "federal_land_leaks_repaired_or_scheduled",
    "average_service_length_feet",
    "excavation_tickets",
    "services_efv_in_system",
    "services_efv_installed",
    "services_shutoff_valve_in_system",
    "services_shutoff_valve_installed",
    "unaccounted_for_gas_percent",
]

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
        YEARLY_DISTRIBUTION_OPERATORS_COLUMNS["columns_to_keep"]
        + YEARLY_DISTRIBUTION_MISC_COLUMNS
        + YEARLY_DISTRIBUTION_FILING_COLUMNS
        + pattern_cols
        # This one gets pulled in into _core_phmsagas__yearly_distribution_by_material_and_size
        + ["main_other_material_detail"]
    )
    untransformed_columns = set(raw_df.columns).difference(transformed_cols)
    if untransformed_columns:
        raise AssertionError(
            f"Found {len(untransformed_columns)} columns that are not incorporated into the transform, but expected none: {untransformed_columns}"
        )


##############################################################################
# PHMSAGAS transformation logic
##############################################################################


@asset(io_manager_key="pudl_io_manager", compute_kind="pandas")
def _core_phmsagas__yearly_distribution_filings(raw_phmsagas__yearly_distribution):
    """Transform information about filings (with PK report_number)."""
    df = raw_phmsagas__yearly_distribution.loc[
        :, YEARLY_DISTRIBUTION_FILING_COLUMNS
    ].copy()

    df = (
        clean_raw_phmsagas__yearly_distribution(df)
        .pipe(standardize_na_values)
        .pipe(pudl.helpers.convert_cols_dtypes)
    )

    # This is just the suffix of the report_number for only a few years.
    # lets check that assumption and then delete it.
    test_log = (
        df.loc[df.log_number.notnull(), ["report_number", "log_number"]]
        .astype(pd.Int64Dtype())
        .astype(str)
    )
    # there was only one that didn't meet this expectation.
    # the log # was 1064 but the report number ended in 1063
    # CG checked and there is another seemingly fully different
    # report_number ended in 1064 so this one seems like the log is wrong
    # so its seems chill to delete this column
    log_not_report_suffix = test_log[
        (~test_log.apply(lambda x: x.report_number.endswith(x.log_number), axis=1))
        & (test_log.report_number != "19951063")
    ]
    if not log_not_report_suffix.empty:
        raise AssertionError(
            f"We expect the log_number is almost always the suffix of the report_number but we found:\n{log_not_report_suffix}"
        )
    df = df.drop(columns=["log_number"])

    df["report_filing_type"] = df["report_filing_type"].str.title()

    # Streamline the initial and supplementary report columns
    df["report_filing_type"] = (
        df["report_filing_type"]
        .mask(df["is_original_filing"], "Initial")
        .mask(df["is_correction_filing"], "Supplemental")
    )
    # TODO: should we drop these?
    # df = df.drop(columns=["is_original_filing", "is_correction_filing"])
    # Standardize telephone and fax number format and drop (000)-000-0000
    df = standardize_phone_column(df, ["preparer_phone", "preparer_fax"])
    return df.drop_duplicates()


@asset(io_manager_key="pudl_io_manager", compute_kind="pandas")
def core_phmsagas__yearly_distribution_operators(
    raw_phmsagas__yearly_distribution: pd.DataFrame,
) -> pd.DataFrame:
    """Pull and transform the yearly distribution PHMSA data into operator-level data.

    Transformations include:

    * Standardize NAs.
    * Strip blank spaces around string values.
    * Convert specific columns to integers.
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

    df = clean_raw_phmsagas__yearly_distribution(df)

    # Standardize NAs
    df = standardize_na_values(df)

    # Convert date columns to datetime
    for col in df.filter(like="date").columns:
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

    return df


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


def clean_raw_phmsagas__yearly_distribution(
    raw_phmsagas__yearly_distribution: pd.DataFrame,
) -> pd.DataFrame:
    """Clean up the raw yearly distribution table for future transforms."""
    cleaned_raw = _dedupe_year_distribution_idx(
        raw_phmsagas__yearly_distribution
    ).replace(
        to_replace={r"^( {1,})$": pd.NA, "NONE": pd.NA, "none": pd.NA}, regex=True
    )
    # there are a small number of records in the older years with a null operator id...
    # sigh. there are already a bunch of records with a placeholder id of 0. so we
    # are setting these null ones to zero.
    assert len(cleaned_raw[cleaned_raw.operator_id_phmsa.isnull()]) >= 20
    cleaned_raw.operator_id_phmsa = cleaned_raw.operator_id_phmsa.fillna(0)

    # Identify non-unique groups based on our core PK. These all have a 0 for
    # the operator_id_phmsa. We check that those are the only ones and
    # then drop them
    non_unique_groups = cleaned_raw[
        cleaned_raw.duplicated(["report_number", "operator_id_phmsa"], keep=False)
    ]
    # There are 6 values that we expect to be duplicated.
    assert len(non_unique_groups) <= 6, (
        f"Found {len(non_unique_groups)} records with duplicates, expected 6 or less."
    )
    assert all(non_unique_groups.operator_id_phmsa == 0), (
        "We expect all of the non-unique rows to have the operator_id_phmsa of 0, but found "
        f"{non_unique_groups.operator_id_phmsa.to_numpy()}"
    )
    # drop the duplicates.
    cleaned_raw = cleaned_raw.drop(non_unique_groups.index)
    return cleaned_raw


def _melt_col_pattern(df, filter_pattern, value_name, col_patterns):
    """Melt a dataframe based on a filter regex pattern and assign pattern columns."""
    return (
        df.set_index(YEARLY_DISTRIBUTION_IDX_ISH)
        .filter(regex=filter_pattern)
        .dropna(how="all", axis="index")
        .convert_dtypes()
        .melt(ignore_index=False, var_name="melt_col", value_name=value_name)
        .pipe(_assign_cols_from_patterns, col_patterns, "melt_col")
        .drop(columns=["melt_col"])
        .set_index(list(col_patterns.keys()), append=True)
        .dropna(how="all", axis="index")
    )


def _melt_merge_main_services(
    raw_phmsagas__yearly_distribution: pd.DataFrame,
    main_pattern: str,
    services_pattern: str,
    col_patterns: dict[str, str],
) -> pd.DataFrame:
    """Filter, melt, add columns then merge miles of main and service."""
    cleaned_raw = clean_raw_phmsagas__yearly_distribution(
        raw_phmsagas__yearly_distribution
    )
    logger.info("Melting main")
    main = _melt_col_pattern(
        cleaned_raw, main_pattern, value_name="mains_miles", col_patterns=col_patterns
    )
    logger.info("Melting service")
    services = _melt_col_pattern(
        cleaned_raw, services_pattern, value_name="services", col_patterns=col_patterns
    )
    return (
        pd.merge(
            main,
            services,
            left_index=True,
            right_index=True,
            how="outer",
            validate="1:1",
            sort=False,
        )
        .reset_index()
        .convert_dtypes()
    )


@asset(io_manager_key="pudl_io_manager", compute_kind="pandas")
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


@asset(io_manager_key="pudl_io_manager", compute_kind="pandas")
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


@asset(io_manager_key="pudl_io_manager", compute_kind="pandas")
def _core_phmsagas__yearly_distribution_by_material_and_size(
    raw_phmsagas__yearly_distribution: pd.DataFrame,
) -> pd.DataFrame:
    """Transform the _core table of the miles of main and services by material type and size.

    This table represents the bulk of the wide raw columns, which means it ends up being
    nearly 8 million records. This transform includes the standard
    ``_melt_merge_main_services`` as well as adding in a column describing the "other"
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
    df = df.merge(
        other_material_detail,
        on=YEARLY_DISTRIBUTION_IDX_ISH + ["material"],
        how="left",
        validate="m:1",
    )
    # okay so an annoying number of stringy info about the material type ended up in the
    # services column across a handful of years. luckily they are easy to ID and move
    # into the right detail description column
    not_float_values = df[pd.to_numeric(df["services"], errors="coerce").isnull()][
        "services"
    ].unique()
    material_detail_wrong_place_mask = (
        df.services.isin(not_float_values) & df.services.notnull()
    )
    # we are about to move these strings into the right place so we wanna make sure we
    # are not replacing info
    assert (
        df[material_detail_wrong_place_mask].main_other_material_detail.isnull().all()
    )
    df.loc[material_detail_wrong_place_mask, "main_other_material_detail"] = df.loc[
        material_detail_wrong_place_mask, "services"
    ]
    df.loc[material_detail_wrong_place_mask, "services"] = np.nan
    df.services = df.services.astype(pd.Float64Dtype())
    return df


@asset(io_manager_key="pudl_io_manager", compute_kind="pandas")
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


@asset(io_manager_key="pudl_io_manager", compute_kind="pandas")
def _core_phmsagas__yearly_distribution_excavation_damages(
    raw_phmsagas__yearly_distribution: pd.DataFrame,
) -> pd.DataFrame:
    """Transform table of damages - broken out by type and sub-type."""
    damage_pattern = MELT_PATTERNS[
        "_core_phmsagas__yearly_distribution_excavation_damages"
    ]["damage_pattern"]
    col_patterns = {
        "damage_type": rf"excavation_damage_({'|'.join(DAMAGE_TYPES_PHMSAGAS)})",
        "damage_sub_type": damage_pattern,
    }
    return (
        clean_raw_phmsagas__yearly_distribution(raw_phmsagas__yearly_distribution)
        .pipe(
            _melt_col_pattern,
            damage_pattern,
            value_name="damages",
            col_patterns=col_patterns,
        )
        .reset_index()
    )


@asset(io_manager_key="pudl_io_manager", compute_kind="pandas")
def _core_phmsagas__yearly_distribution_misc(
    raw_phmsagas__yearly_distribution: pd.DataFrame,
) -> pd.DataFrame:
    """Transform this distribution table of miscellaneous numeric values.

    TODO: ADD summary information about incidents (also conceptually reported on the basis of report year, state, and operator ID)
    """
    idx = ["report_year", "report_number", "operator_id_phmsa", "operating_state"]
    df = (
        clean_raw_phmsagas__yearly_distribution(raw_phmsagas__yearly_distribution)
        .loc[:, idx + YEARLY_DISTRIBUTION_MISC_COLUMNS]
        .sort_values(YEARLY_DISTRIBUTION_MISC_COLUMNS, ascending=False)
    )
    # there are 6 dupes in this paired down PK. they all have an operator id of 0
    # and are from 1980 or 1981. only one of these records have non-zero or non-null
    # data in them at all. We sort right b4 this to get that one record in our drop
    # dupes/first after some checks
    dupes = df[df.duplicated(subset=idx, keep=False)]
    if not dupes.empty:
        assert all(dupes.operator_id_phmsa == 0)
        assert all(dupes.report_year.isin([1980, 1981]))
        assert len(dupes) <= 6
    df = df.drop_duplicates(subset=idx, keep="first")

    # Convert percent unaccounted for gas to a fraction
    df["unaccounted_for_gas_fraction"] = df["unaccounted_for_gas_percent"] / 100
    df = df.drop(columns=["unaccounted_for_gas_percent"])

    return df.convert_dtypes()


@asset_check(asset=_core_phmsagas__yearly_distribution_misc, blocking=True)
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
