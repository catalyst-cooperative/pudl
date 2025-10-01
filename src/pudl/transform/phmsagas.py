"""Classes & functions to process PHMSA natural gas data before loading into the PUDL DB."""

import numpy as np
import pandas as pd
from dagster import asset, asset_check

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
    "report_id",
    "log_number",  # we drop this after a check
    "operator_id_phmsa",
    "report_date",
    "operating_state",
    "filing_date",
    "initial_filing_date",
    "filing_correction_date",
    "report_filing_type",
    "data_date",
    "is_original_filing",  # we drop this after condensing into report_filing_type
    "is_correction_filing",  # we drop this after condensing into report_filing_type
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
        "report_id",
        "supplemental_report_id",
        "report_date",
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
        "report_id",
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
    "report_date",
    "report_id",
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
        + [  # This one gets pulled in into _core_phmsagas__yearly_distribution_by_material_and_size
            "main_other_material_detail",
            # we convert report_year into report_date immediately in _core_phmsagas__yearly_distribution
            "report_year",
        ]
    )
    untransformed_columns = set(raw_df.columns).difference(transformed_cols)
    if untransformed_columns:
        raise AssertionError(
            f"Found {len(untransformed_columns)} columns that are not incorporated into the transform, but expected none: {untransformed_columns}"
        )


##############################################################################
# PHMSAGAS transformation logic
##############################################################################


def _dedupe_year_distribution_idx(
    raw_phmsagas__yearly_distribution: pd.DataFrame,
) -> pd.DataFrame:
    """Remove the rare duplicates in the expected primary key.

    There are 51 found records which have duplicate values for the expected
    primary key of this table. Many manipulations are much easier when we have a
    unique primary key - merges for instance! So we want to remove these duplicates.

    On visual inspection, these duplicates either look mostly the same or have one
    record with most or all of the non-null or non-zero values. Therefore, we sort
    the total columns and then drop duplicates so we kept the records which have the
    most information. This is not the most robust method of de-duplicating but there
    are so few records compared to the >90k total records.

    Then there are a few extra fun records that are still duplicated. These all have
    the know null-like operator_id_phmsa of 0 so we are going to drop them.
    """
    tot_cols = list(raw_phmsagas__yearly_distribution.filter(like="_total").columns)
    raw_phmsagas__yearly_distribution = raw_phmsagas__yearly_distribution.sort_values(
        by=tot_cols, ascending=False
    )
    dupe_mask = raw_phmsagas__yearly_distribution.duplicated(
        subset=YEARLY_DISTRIBUTION_IDX_ISH, keep=False
    )
    assert len(raw_phmsagas__yearly_distribution[dupe_mask]) <= 51, (
        f"Found {len(raw_phmsagas__yearly_distribution[dupe_mask])} duplicates "
        "on the expected core PKs, but expected 51 or less"
    )
    cleaned_raw = pd.concat(
        [
            raw_phmsagas__yearly_distribution[~dupe_mask],
            raw_phmsagas__yearly_distribution[dupe_mask].drop_duplicates(
                subset=YEARLY_DISTRIBUTION_IDX_ISH, keep="first"
            ),
        ],
    )
    # Identify non-unique groups based on our core PK. These all have a 0 for
    # the operator_id_phmsa. We check that those are the only ones and
    # then drop them
    non_unique_groups = cleaned_raw[
        cleaned_raw.duplicated(["report_id", "operator_id_phmsa"], keep=False)
    ]
    # There are 6 values that we expect to be duplicated.
    assert len(non_unique_groups) < 7, (
        f"Found {len(non_unique_groups)} records with duplicates, expected 6 or less."
    )
    assert all(non_unique_groups.operator_id_phmsa == 0), (
        "We expect all of the non-unique rows to have the operator_id_phmsa of 0, but found "
        f"{non_unique_groups.operator_id_phmsa.to_numpy()}"
    )
    # drop the duplicates.
    cleaned_raw = cleaned_raw.drop(non_unique_groups.index)
    return cleaned_raw


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


def backfill_zero_operator_id_phmsa(df: pd.DataFrame) -> pd.DataFrame:
    """Backfill some of the 0's in the operator_id_phmsa.

    We are trying to backfill PHMSA's version of null operator_id_phmsa's which is 0.
    These 0's show up particularly in the pre-1990's years of data. It would be ideal
    to figure out ways to confidently replace all of the 0's, but for now we are only
    replacing 0's when the operator_name_phmsa is exactly the same.
    """
    og_zeros = (df.operator_id_phmsa == 0).sum()
    filled_in = (
        df.sort_values(["report_date"])
        .assign(
            operator_id_phmsa_old=lambda x: x.operator_id_phmsa,  # just for debugging
            operator_name_phmsa=lambda y: y.operator_name_phmsa.str.title(),
        )
        .replace({"operator_id_phmsa": {0: pd.NA}})
        .assign(
            operator_id_phmsa=lambda z: z.operator_id_phmsa.fillna(
                z.groupby(["operator_name_phmsa"], dropna=True)[
                    "operator_id_phmsa"
                ].bfill()
            ).fillna(0)
        )
        # this is just for our eyeballs - it sorts back to how it was before the sorting by date
        .sort_index()
    )
    still_zeros = (filled_in.operator_id_phmsa == 0).sum()
    logger.info(
        f"Backfilled {(og_zeros - still_zeros) / og_zeros:.1%} of zero's in operator_id_phmsa"
    )
    return filled_in


@asset
def _core_phmsagas__yearly_distribution(
    raw_phmsagas__yearly_distribution: pd.DataFrame,
) -> pd.DataFrame:
    """Clean up the raw yearly distribution table for future transforms.

    This function mostly deduplicates the records on the core primary key.
    """
    # there are a small number of records in the older years with a null operator id...
    # sigh. there are already a bunch of records with a placeholder id of 0. so we
    # are setting these null ones to zero.
    assert (
        len(
            baddies := raw_phmsagas__yearly_distribution[
                raw_phmsagas__yearly_distribution.operator_id_phmsa.isnull()
            ]
        )
        <= 20
    ), baddies
    raw_phmsagas__yearly_distribution.operator_id_phmsa = (
        raw_phmsagas__yearly_distribution.operator_id_phmsa.fillna(0)
    )

    cleaned_raw = (
        raw_phmsagas__yearly_distribution.assign(
            report_date=lambda x: pd.to_datetime(x["report_year"], format="%Y")
        )
        .drop(columns=["report_year"])
        .pipe(standardize_na_values)
        .pipe(backfill_zero_operator_id_phmsa)
        .pipe(_dedupe_year_distribution_idx)
    )

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
    cleaned_raw: pd.DataFrame,
    main_pattern: str,
    services_pattern: str,
    col_patterns: dict[str, str],
) -> pd.DataFrame:
    """Filter, melt, add columns then merge miles of main and service."""
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


def _check_and_drop_log_if_always_in_report_id(df):
    """Check to ensure we can drop the log column w/o losing information.

    The log column is only reported for a few years and we assume its
    duplicative because its just the suffix of report_id. This function
    checks that assumption and then deletes the log column.
    """
    test_log = (
        df.loc[df.log_number.notnull(), ["report_id", "log_number"]]
        .astype(pd.Int64Dtype())
        .astype(str)
    )
    # there was only one that didn't meet this expectation.
    # the log # was 1064 but the report number ended in 1063
    # CG checked and there is another seemingly fully different
    # report_id ended in 1064 so this one seems like the log is wrong
    # so its seems chill to delete this column
    log_not_report_suffix = test_log[
        (~test_log.apply(lambda x: x.report_id.endswith(x.log_number), axis=1))
        & (test_log.report_id != "19951063")
    ]
    if not log_not_report_suffix.empty:
        raise AssertionError(
            f"We expect the log_number is almost always the suffix of the report_id but we found:\n{log_not_report_suffix}"
        )
    df = df.drop(columns=["log_number"])
    return df


@asset(io_manager_key="pudl_io_manager", compute_kind="pandas")
def _core_phmsagas__yearly_distribution_filings(
    _core_phmsagas__yearly_distribution: pd.DataFrame,
) -> pd.DataFrame:
    """Transform information about filings (with PK report_id)."""
    df = (
        _core_phmsagas__yearly_distribution.loc[:, YEARLY_DISTRIBUTION_FILING_COLUMNS]
        .pipe(standardize_na_values)
        .pipe(pudl.helpers.convert_cols_dtypes)
        .pipe(_check_and_drop_log_if_always_in_report_id)
        # Streamline the initial and supplementary report columns
        .assign(
            report_filing_type=lambda z: (
                z["report_filing_type"]
                .mask(
                    (z.is_original_filing == "Y") & z.report_filing_type.isnull(),
                    "Initial",
                )
                .mask(
                    (z.is_correction_filing == "N") & z.report_filing_type.isnull(),
                    "Supplemental",
                )
            ).str.title(),
        )
        .drop(columns=["is_original_filing", "is_correction_filing"])
        # Standardize telephone and fax number format and drop (000)-000-0000
        .pipe(standardize_phone_column, ["preparer_phone", "preparer_fax"])
        .drop_duplicates()
    )
    return df


@asset(io_manager_key="pudl_io_manager", compute_kind="pandas")
def core_phmsagas__yearly_distribution_operators(
    _core_phmsagas__yearly_distribution: pd.DataFrame,
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
    df = _core_phmsagas__yearly_distribution.loc[
        :, YEARLY_DISTRIBUTION_OPERATORS_COLUMNS["columns_to_keep"]
    ].pipe(standardize_na_values)

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


@asset(io_manager_key="pudl_io_manager", compute_kind="pandas")
def _core_phmsagas__yearly_distribution_by_material(
    _core_phmsagas__yearly_distribution: pd.DataFrame,
) -> pd.DataFrame:
    """Transform the _core table of the miles of main and services by material."""
    return _melt_merge_main_services(
        _core_phmsagas__yearly_distribution,
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
    _core_phmsagas__yearly_distribution: pd.DataFrame,
) -> pd.DataFrame:
    """Transform the _core table of the miles of main and services by decade."""
    return _melt_merge_main_services(
        _core_phmsagas__yearly_distribution,
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
    _core_phmsagas__yearly_distribution: pd.DataFrame,
) -> pd.DataFrame:
    """Transform the _core table of the miles of main and services by material type and size.

    This table represents the bulk of the wide raw columns, which means it ends up being
    nearly 8 million records. This transform includes the standard
    ``_melt_merge_main_services`` as well as adding in a column describing the "other"
    material type (``main_other_material_detail``).

    This table takes by far the longest to generate because of how large it is.
    """
    df = _melt_merge_main_services(
        _core_phmsagas__yearly_distribution,
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
    other_material_detail = _core_phmsagas__yearly_distribution.assign(
        material="other"
    )[YEARLY_DISTRIBUTION_IDX_ISH + ["material", "main_other_material_detail"]].dropna(
        subset=["main_other_material_detail"]
    )
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
    _core_phmsagas__yearly_distribution: pd.DataFrame,
) -> pd.DataFrame:
    """Transform table of leaks - broken out by source and leak severity."""
    return _melt_merge_main_services(
        _core_phmsagas__yearly_distribution,
        MELT_PATTERNS["_core_phmsagas__yearly_distribution_leaks"]["main_pattern"],
        MELT_PATTERNS["_core_phmsagas__yearly_distribution_leaks"]["services_pattern"],
        {
            "leak_severity": r"^(all_leaks|hazardous_leaks)",
            "leak_source": r"^(?:all_leaks|hazardous_leaks)_(.*)_(?:mains|services)",
        },
        # All of the other tables with mains in it have the miles of main except for
        # this one. We could totally edit _melt_merge_main_services to enable different
        # column names instead of doing this rename
    ).rename(columns={"mains_miles": "mains"})


@asset(io_manager_key="pudl_io_manager", compute_kind="pandas")
def _core_phmsagas__yearly_distribution_excavation_damages(
    _core_phmsagas__yearly_distribution: pd.DataFrame,
) -> pd.DataFrame:
    """Transform table of damages - broken out by type and sub-type."""
    damage_pattern = MELT_PATTERNS[
        "_core_phmsagas__yearly_distribution_excavation_damages"
    ]["damage_pattern"]
    col_patterns = {
        "damage_type": rf"excavation_damage_({'|'.join(DAMAGE_TYPES_PHMSAGAS)})",
        "damage_sub_type": damage_pattern,
    }
    return _core_phmsagas__yearly_distribution.pipe(
        _melt_col_pattern,
        damage_pattern,
        value_name="damages",
        col_patterns=col_patterns,
    ).reset_index()


@asset(io_manager_key="pudl_io_manager", compute_kind="pandas")
def _core_phmsagas__yearly_distribution_misc(
    _core_phmsagas__yearly_distribution: pd.DataFrame,
) -> pd.DataFrame:
    """Transform this distribution table of miscellaneous numeric values."""
    idx = ["report_date", "report_id", "operator_id_phmsa", "operating_state"]
    df = (
        _core_phmsagas__yearly_distribution.loc[
            :, idx + YEARLY_DISTRIBUTION_MISC_COLUMNS
        ]
        .sort_values(YEARLY_DISTRIBUTION_MISC_COLUMNS, ascending=False)
        # Convert percent unaccounted for gas to a fraction
        .assign(
            unaccounted_for_gas_fraction=lambda x: x.unaccounted_for_gas_percent / 100
        )
        .drop(columns=["unaccounted_for_gas_percent"])
        .convert_dtypes()
    )
    return df
