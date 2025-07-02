"""Transformations and standardizations for SEC 10-K data tables.

This module contains routines for reshaping, cleaning, and standardizing the raw SEC
10-K data into normalized core tables. These core tables serve as the building blocks
for denormalized output tables.
"""

import dagster as dg
import pandas as pd

from pudl import logging_helpers
from pudl.analysis.record_linkage import name_cleaner
from pudl.metadata.dfs import ALPHA_2_COUNTRY_CODES, SEC_EDGAR_STATE_AND_COUNTRY_CODES

logger = logging_helpers.get_logger(__name__)


######################################################################
### Helper functions for cleaning and reshaping the SEC 10-K data. ###
######################################################################
def _year_quarter_to_date(year_quarter: pd.Series) -> pd.Series:
    """Convert a year quarter in the format '2024q1' to date type."""
    return pd.PeriodIndex(year_quarter, freq="Q").to_timestamp()


def _simplify_filename_sec10k(filename_sec10k: pd.Series) -> pd.Series:
    """Strip non-unique path and file type extension from partial SEC 10-K filenames.

    The full source URL can be constructed by prepending
    https://www.sec.gov/Archives/edgar/data/ and adding back the ".txt" file type
    extension, as is done for the source_url column in the output tables.
    """
    return (
        filename_sec10k.str.removeprefix("edgar/data/")
        .str.removesuffix(".txt")
        .astype("string")
    )


def _extract_filer_cik_from_filename(filename_col: pd.Series) -> pd.Series:
    """Extract filer company's central index key from the first token of filename."""
    return filename_col.apply(lambda x: x.split("/")[0]).str.zfill(10)


def _compute_fraction_owned(percent_ownership: pd.Series) -> pd.Series:
    """Clean percent ownership, convert to float, then convert percent to ratio."""
    return (
        percent_ownership.str.replace(r"(\.{2,})", r"\.", regex=True)
        .replace("\\\\", "", regex=True)
        .replace(".", "0.0", regex=False)
        .astype("float")
    ) / 100.0


def _standardize_taxpayer_id_irs(taxpayer_id_irs: pd.Series) -> pd.Series:
    """Standardize the IRS taxpayer ID number to NN-NNNNNNN format.

    - Remove all non-numeric characters
    - Set all values that are not 9 digits long or are entirely zeroes to pd.NA
    - Reformat to NN-NNNNNNN format.
    """
    tin = taxpayer_id_irs.astype("string").str.replace(r"[^\d]", "", regex=True)
    not_nine_digits = ~tin.str.match(r"^\d{9}$")
    logger.info(f"Nulling {sum(not_nine_digits.dropna())} invalid TINs.")
    nine_zeroes = tin == "000000000"
    logger.info(f"Nulling {sum(nine_zeroes.dropna())} all-zero TINs.")
    tin.loc[(not_nine_digits | nine_zeroes)] = pd.NA
    tin = tin.str[:2] + "-" + tin.str[2:]
    return tin


def _standardize_industrial_classification(sic: pd.Series) -> pd.DataFrame:
    """Split industry names and codes into separate columns.

    Most values take the form of "Industry description [1234]", but some are just
    4-digit numbers. This function splits the industry name and code into separate
    columns, and fills in the code column with the value from the
    standard_industrial_classification column if it is a 4-digit number. Any values
    that don't fit either of these two patterns are set to NA. No effort is made to
    fill in missing industry descriptions based on the industry code.

    See e.g. https://www.osha.gov/data/sic-manual for code definitions."
    """
    sic_df = pd.DataFrame()
    sic = sic.str.strip()
    sic_df[["industry_name_sic", "industry_id_sic"]] = sic.str.extract(
        r"^(.+)\[(\d{4})\]$"
    )
    sic_df["industry_name_sic"] = sic_df["industry_name_sic"].str.strip()
    sic_name_zero = sic_df["industry_name_sic"].str.contains("0000").fillna(False)
    sic_df.loc[sic_name_zero, "industry_name_sic"] = pd.NA
    sic_df["industry_id_sic"] = sic_df["industry_id_sic"].fillna(
        sic.where(sic.str.match(r"^\d{4}$"), pd.NA)
    )
    sic_id_zero = (sic_df["industry_id_sic"] == "0000").fillna(False)
    sic_df.loc[sic_id_zero, "industry_id_sic"] = pd.NA
    return sic_df


def _pivot_info_block(df: pd.DataFrame, block: str) -> pd.DataFrame:
    """Select and pivot distinct blocks of company information for further processing.

    Args:
        df: The dataframe containing the SEC 10-K company information.
        block: The block of associated information to pivot.
    """
    pivoted = (
        df.loc[df["block"] == block]
        .pivot(
            values="value",
            index=[
                "filename_sec10k",
                "filer_count",
                "block_count",
            ],
            columns="key",
        )
        .convert_dtypes()
    )
    pivoted.columns.name = None
    return pivoted


def _standardize_company_name(col: pd.Series) -> pd.Series:
    """Clean a company name column and standardize legal terms.

    Uses the PUDL name cleaner object to do basic cleaning on `col_name` column
    such as stripping punctuation, correcting case, and normalizing legal
    terms, i.e. llc -> limited liability company.

    Arguments:
        col: The series of names that is to be cleaned.

    Returns:
        The original Series now containing cleaned names.
    """
    col = col.fillna(pd.NA).str.strip().str.lower()
    company_name_cleaner = name_cleaner.CompanyNameCleaner(
        cleaning_rules_list=[
            "replace_ampersand_in_spaces_by_AND",
            "replace_hyphen_by_space",
            "replace_underscore_by_space",
            "remove_text_punctuation",
            "remove_parentheses",
            "remove_brackets",
            "remove_curly_brackets",
            "remove_words_between_slashes",
            "enforce_single_space_between_words",
        ],
        legal_term_location=2,
    )
    col = company_name_cleaner.apply_name_cleaning(col).str.strip()
    col = col.replace("", pd.NA)

    return col


def _remove_bad_subsidiary_names(col: pd.Series) -> pd.Series:
    """Remove subsidiary names that are obviously erroneous.

    The model makes common mistakes while extracting subsidiary
    names from Ex. 21 filings, such as extracting "llc" as a complete
    name. Remove names that are just legal terms as well as a list
    of other common erroneous names. Replace these bad names with
    NaNs.
    """
    legal_term_remover = name_cleaner.CompanyNameCleaner(
        cleaning_rules_list=[], handle_legal_terms=2
    )
    clean_col = legal_term_remover.apply_name_cleaning(col).str.strip()
    # some common bad names that aren't removed by the legal term remover
    clean_col = clean_col.replace(
        [
            "the",
            "",
            "gmbh",
            "i",
            "name",
            "of",
            "branch",
            "international",
            "partnership",
            "s.a",  # spanish language legal term
            "b.v",
            "c.v",  # spanish language legal term
            "services",
            "holdings",
        ],
        pd.NA,
    )
    return col.where(~clean_col.isnull(), pd.NA)


def _standardize_location(loc_col: pd.Series) -> pd.Series:
    """Map two letter state codes to full names using EDGAR state and country code mapping."""
    out = (
        loc_col.str.strip()
        .str.lower()
        .replace(
            SEC_EDGAR_STATE_AND_COUNTRY_CODES.set_index("state_or_country_code")[
                "state_or_country_name"
            ]
        )
        .replace(ALPHA_2_COUNTRY_CODES.set_index("country_code")["country_name"])
        .fillna(pd.NA)
        .replace("", pd.NA)
    )
    return out


def _match_ex21_subsidiaries_to_filer_company(
    filer_info_df: pd.DataFrame,
    ownership_df: pd.DataFrame,
) -> pd.DataFrame:
    """Match Exhibit 21 subsidiaries to companies that file SEC Form 10-K.

    We want to assign CIKs to Ex. 21 subsidiaries if they in turn file a 10k. To do
    this, we merge the Ex. 21 subsidiaries to 10k filers on company name. If there are
    multiple matches with the same company name we choose the pair with the most overlap
    in location of incorporation and nearest report years. Then we merge the CIK back
    onto the Ex. 21 DataFrame.

    Returns:
        A dataframe of the Ex. 21 subsidiaries with a column for the subsidiaries CIK
        (null if the subsidiary doesn't file).
    """
    filer_info_df["subsidiary_company_name"] = _standardize_company_name(
        filer_info_df["company_name"]
    )
    ownership_df["subsidiary_company_name"] = _standardize_company_name(
        ownership_df["subsidiary_company_name"]
    )
    filer_info_df = filer_info_df.dropna(subset="subsidiary_company_name")
    ownership_df = ownership_df.dropna(subset="subsidiary_company_name")
    # expand two letter states codes to full location names to match
    # the subsidiary_company_location column of the ownership table
    filer_info_df["incorporation_state"] = _standardize_location(
        filer_info_df["incorporation_state"]
    )
    merged_df = filer_info_df.merge(
        ownership_df[
            [
                "subsidiary_company_name",
                "subsidiary_company_id_sec10k",
                "subsidiary_company_location",
                "report_date",
            ]
        ],
        how="inner",
        on="subsidiary_company_name",
        suffixes=("_sec", "_ex21"),
        validate="many_to_many",
    )
    # split up the location of incorporation on whitespace, creating
    # sets of word tokens
    words1 = (
        merged_df["incorporation_state"]
        .fillna("")
        .str.lower()
        .str.split()
        .apply(lambda words: set(words) if words != [""] else set())
    )
    words2 = (
        merged_df["subsidiary_company_location"]
        .fillna("")
        .str.lower()
        .str.split()
        .apply(lambda words: set(words) if words != [""] else set())
    )
    # Note that these dataframe column assignments to lists work based on the ORDER
    # of the rows/list elements, not the index, so it's important that no reordering
    # happen in here.
    intersection_lens = [
        len(w1.intersection(w2)) for w1, w2 in zip(words1, words2, strict=True)
    ]
    max_lens = [max(len(w1), len(w2), 1) for w1, w2 in zip(words1, words2, strict=True)]
    # Don't convert the list to a Series before assignment to avoid accidentally having it align
    # based on index rather than order
    merged_df["loc_overlap"] = [
        i / m for i, m in zip(intersection_lens, max_lens, strict=True)
    ]
    # get the difference in report dates
    merged_df["report_date_diff_days"] = abs(
        (merged_df["report_date_sec"] - merged_df["report_date_ex21"]).dt.days
    )
    merged_df = merged_df.sort_values(
        by=[
            "subsidiary_company_name",
            "subsidiary_company_location",
            "loc_overlap",
            "report_date_diff_days",
        ],
        ascending=[True, True, False, True],
    )
    # Select the filer with the highest loc overlap and nearest report dates
    # for each subsidiary_company_id_sec10k
    closest_match_df = merged_df.groupby(
        ["subsidiary_company_id_sec10k"],
        as_index=False,
    ).first()
    ownership_with_cik_df = ownership_df.merge(
        closest_match_df[
            [
                "subsidiary_company_id_sec10k",
                "central_index_key",
            ]
        ],
        how="left",
        on=["subsidiary_company_id_sec10k"],
        validate="many_to_one",
    ).rename(columns={"central_index_key": "subsidiary_company_central_index_key"})

    return ownership_with_cik_df


#######################################
### SEC 10-K core asset definitions ###
#######################################
@dg.asset(
    io_manager_key="pudl_io_manager",
    group_name="core_sec10k",
)
def core_sec10k__quarterly_filings(
    raw_sec10k__quarterly_filings: pd.DataFrame,
) -> pd.DataFrame:
    """Standardize the contents of the SEC 10-K filings table."""
    filings = raw_sec10k__quarterly_filings.rename(
        columns={
            "sec10k_filename": "filename_sec10k",
            "form_type": "sec10k_type",
            "date_filed": "filing_date",
        }
    ).assign(
        filename_sec10k=lambda x: _simplify_filename_sec10k(x["filename_sec10k"]),
        report_date=lambda x: _year_quarter_to_date(x["year_quarter"]),
        filing_date=lambda x: pd.to_datetime(x["filing_date"]),
        central_index_key=lambda x: x["central_index_key"].str.zfill(10),
        # Lower case these fields so they are the same as in other tables.
        company_name=lambda x: x["company_name"].str.lower(),
        sec10k_type=lambda x: x["sec10k_type"].str.lower(),
    )
    return filings


@dg.multi_asset(
    outs={
        "_core_sec10k__quarterly_company_information": dg.AssetOut(
            group_name="core_sec10k",
        ),
        "_core_sec10k__changelog_company_name": dg.AssetOut(
            group_name="core_sec10k",
        ),
    }
)
def core_sec10k__company_info(
    raw_sec10k__quarterly_company_information: pd.DataFrame,
):
    """Reshape data from the raw SEC 10-K company information table.

    Each SEC 10-K filing contains a header block that lists information the filer, and
    potentially a number of other related companies. Each company level block of
    information may include the company's name, physical address, some details about
    how it files the SEC 10-K, and some other company attributes like taxpayer ID
    number, industrial classification, etc. It may also include a mailing address that
    is distinct from the physical address, and the history of names that the company has
    had over time.

    In its original form, this data is mostly contained in two poorly normalized "key"
    and "value" columns, with the key indicating what kind of data will be contained in
    the value column. Here we pivot the keys into column names so that each column has
    a homogeneous type of value.

    Each filing may contain information about multiple companies, with each identified
    by a different filer_count. For each company there are several different potential
    types of information provide in distinct named "blocks" (business_address,
    company_data, filing_values, mail_address, and former_company). Each block type
    needs to be extracted and pivoted independently to avoid column name collisions and
    because they don't all have the same set of block_count and filer_count values.

    After they've been pivoted, the blocks are merged together into two disinct tables:

    - per-filing company and filing information
    - the history of company name changes

    This information is used elsewhere for matching SEC 10-K filers to EIA utilities
    and for identifying the companies that are mentioned in the unstructured Exhibit 21
    attachments.

    There is a 1-to-1 correspondence between the company_data and filing_values blocks,
    and virtually all business_address blocks (99.9%), but a substantial number (~5%) of
    mail_address blocks do not share index values with any company_data or filing_values
    blocks, and are ultimately dropped. The former_company blocks contain a large amount
    of duplicative data, but can be associated with a central_index_key value for later
    association with an individual SEC 10-K filer.

    Further processing that standardizes the contents of these tables is deferred to
    separate downstream core assets.
    """
    raw_company_info = raw_sec10k__quarterly_company_information.rename(
        columns={
            "sec10k_filename": "filename_sec10k",
        }
    ).assign(
        key=lambda x: x["key"].str.lstrip("]"),
        filename_sec10k=lambda x: _simplify_filename_sec10k(x["filename_sec10k"]),
    )

    business_address = _pivot_info_block(
        raw_company_info, block="business_address"
    ).rename(columns={"business_phone": "phone"})
    company_data = _pivot_info_block(raw_company_info, block="company_data").drop(
        columns=["organization_name"]
    )
    filing_values = _pivot_info_block(raw_company_info, block="filing_values")
    mail_address = _pivot_info_block(raw_company_info, block="mail_address")
    former_company = _pivot_info_block(raw_company_info, block="former_company")

    # Add prefixes where needed to ensure that column names are distinct after concatenation.
    business_address.columns = [f"business_{col}" for col in business_address.columns]
    mail_address.columns = [f"mail_{col}" for col in mail_address.columns]

    # There should be a 1:1 relationship between the filing and company data.
    assert (filing_values.sort_index().index == company_data.sort_index().index).all()

    company_info = (
        pd.merge(
            left=company_data.sort_index(),
            right=filing_values.sort_index(),
            left_index=True,
            right_index=True,
            how="left",
            validate="one_to_one",
        )
        .merge(  # 386 business addresses orphaned here.
            business_address.sort_index(),
            left_index=True,
            right_index=True,
            how="left",
            validate="one_to_one",
        )
        .merge(  # 22,612 mailing addresses orphaned here.
            mail_address.sort_index(),
            left_index=True,
            right_index=True,
            how="left",
            validate="one_to_one",
        )
        .reset_index()
    )
    # If this isn't unique then our assumption about the merges above is wrong.
    assert company_info.index.is_unique
    # Make sure we aren't suddenly losing more address records here than expected
    orphan_biz_address = business_address.loc[
        business_address.index.difference(company_data.index)
    ]
    assert len(orphan_biz_address) <= 386
    orphan_mail_address = mail_address.loc[
        mail_address.index.difference(company_data.index)
    ]
    assert len(orphan_mail_address) <= 22_612

    # This merge is necessary to ensure that the company name changelog table has access
    # to the current company name that was found in the associated filing. Intuitively
    # we would want to merge this information in based on the CIK in the output table,
    # but we don't have a well normalized table associating all CIKs with the
    # corresponding company name as a function of time -- this is the information that
    # we are trying to glean from the company name changelog.
    name_changes = pd.merge(
        left=company_data[["central_index_key", "company_conformed_name"]],
        right=former_company,
        left_index=True,
        right_index=True,
        how="inner",
        validate="one_to_one",
    ).reset_index()

    return (
        dg.Output(
            output_name="_core_sec10k__quarterly_company_information",
            value=company_info,
        ),
        dg.Output(
            output_name="_core_sec10k__changelog_company_name",
            value=name_changes,
        ),
    )


@dg.asset(
    io_manager_key="pudl_io_manager",
    group_name="core_sec10k",
)
def core_sec10k__quarterly_company_information(
    _core_sec10k__quarterly_company_information: pd.DataFrame,
) -> pd.DataFrame:
    """Clean and standardize the contents of the SEC 10-K company information table."""
    clean_info = _core_sec10k__quarterly_company_information.rename(
        columns={
            "company_conformed_name": "company_name",
            "irs_number": "taxpayer_id_irs",
            "state_of_incorporation": "incorporation_state",
            "form_type": "sec10k_type",
            "sec_file_number": "filing_number_sec",
            "business_phone": "phone_number",
            "business_street_1": "business_street_address",
            "business_street_2": "business_street_address_2",
            "mail_street_1": "mail_street_address",
            "mail_street_2": "mail_street_address_2",
        }
    ).assign(
        business_state=lambda x: x.business_state.str.upper(),
        mail_state=lambda x: x.mail_state.str.upper(),
        incorporation_state=lambda x: x.incorporation_state.str.upper(),
    )
    # Standardize US ZIP codes and save apparent non-US postal codes in their own column
    for zip_col in ["business_zip", "mail_zip"]:
        zip5_mask = clean_info[zip_col].str.match(r"^\d{5}$")
        zip9_mask = clean_info[zip_col].str.match(r"^\d{9}$")
        zip54_mask = clean_info[zip_col].str.match(r"^\d{5}-\d{4}$")
        zip_mask = zip5_mask | zip9_mask | zip54_mask

        clean_info[f"{zip_col}_clean"] = clean_info.loc[
            zip5_mask | zip54_mask, [zip_col]
        ]
        clean_info.loc[zip9_mask, f"{zip_col}_clean"] = (
            clean_info.loc[zip9_mask, zip_col].str[:5]
            + "-"
            + clean_info.loc[zip9_mask, zip_col].str[5:]
        )
        clean_info[f"{zip_col.split('_')[0]}_postal_code"] = clean_info.loc[
            ~zip_mask, [zip_col]
        ]
        clean_info[[f"{zip_col}_code", f"{zip_col}_code_4"]] = clean_info[
            f"{zip_col}_clean"
        ].str.split("-", expand=True)
        clean_info = clean_info.drop(columns=[zip_col, f"{zip_col}_clean"])

    clean_info[["industry_name_sic", "industry_id_sic"]] = (
        _standardize_industrial_classification(
            clean_info["standard_industrial_classification"]
        )
    )

    # Use a single standard value for the SEC Act column
    clean_info["sec_act"] = clean_info["sec_act"].replace("34", "1934 act")

    # Ensure fiscal year end conforms to MMDD format and specifies a valid date.
    mmdd_regex = r"^(?:(?:0[1-9]|1[0-2])(?:0[1-9]|1\d|2\d|3[01])|(?:0[13-9]|1[0-2])(?:29|30)|(?:0[13578]|1[02])31)$"
    # Fix a handful of typos
    clean_info["fiscal_year_end"] = (
        clean_info["fiscal_year_end"].str.zfill(4).replace("0939", "0930")
    )
    # Set the irredemable values to NA
    invalid_fye_mask = ~clean_info["fiscal_year_end"].str.match(mmdd_regex)
    assert sum(invalid_fye_mask.dropna()) < 5
    clean_info.loc[invalid_fye_mask, "fiscal_year_end"] = pd.NA

    clean_info["taxpayer_id_irs"] = _standardize_taxpayer_id_irs(
        clean_info["taxpayer_id_irs"]
    )

    # Remove invalid 10-k form types. The only observed bad values are "th"
    invalid_sec10k_type = clean_info["sec10k_type"] == "th"
    assert sum(invalid_sec10k_type.dropna()) < 11
    clean_info.loc[invalid_sec10k_type, "sec10k_type"] = pd.NA

    # Enforce filename_sec10k and central_index_key as a primary key.
    # Occasionally a file will contain two blocks of info for the same
    # CIK. Ensure that records with duplicate values of filename_sec10k and
    # central_index_key differ ONLY in that some of them may have
    # null values that are non-null in some other duplicate,
    # OR that they have different values for filer_count, block_count
    # or film_number -- which are not meaningful information and
    # we're okay losing one of the original values in those columns.
    cols_to_fill = clean_info.columns.drop(
        ["filer_count", "film_number", "block_count"]
    )
    dupes_df = clean_info[
        clean_info.duplicated(
            subset=["filename_sec10k", "central_index_key"], keep=False
        )
    ]
    if len(dupes_df) > 0:
        filled_dupes = dupes_df.groupby(["filename_sec10k", "central_index_key"])[
            cols_to_fill
        ].transform(lambda group: group.ffill().bfill())
        clean_info.loc[filled_dupes.index, cols_to_fill] = filled_dupes
        clean_info = clean_info.drop_duplicates(subset=cols_to_fill)
    # After we've filled in the NA values and dropped duplicates
    # on the full set of columns we care about preserving,
    # we expect that there will only be unique values of the natural PK
    n_remaining_dupes = len(
        clean_info[
            clean_info.duplicated(subset=["filename_sec10k", "central_index_key"])
        ]
    )
    if n_remaining_dupes > 0:
        raise AssertionError(
            logger.error(
                f"There are {n_remaining_dupes} duplicate filename_sec10k and central_index_key "
                "company info blocks in the core_sec10k__quarterly_company_information table "
                "after filling null values. Filename and CIK should be a primary key."
            )
        )

    return clean_info


@dg.asset(
    io_manager_key="pudl_io_manager",
    group_name="core_sec10k",
)
def core_sec10k__changelog_company_name(
    _core_sec10k__changelog_company_name: pd.DataFrame,
) -> pd.DataFrame:
    """Clean and standardize the SEC 10-K company name changelog table.

    Every filing that references a company independently reports the history of its name
    changes, so after combining them we end up with many copies of the name change
    histories for some companies. We deduplicate the combined history and keep only a
    single instance of each reported name change.

    There are a handful of cases in which the same name change events seem to have been
    reported differently in different filings (slightly different company names or
    dates) which make the results here imperfect, but for the most part it works well.
    """
    name_changelog = (
        _core_sec10k__changelog_company_name.assign(
            name_change_date=lambda x: pd.to_datetime(x["date_of_name_change"])
        )
        .rename(
            columns={
                "former_conformed_name": "company_name_old",
                "company_conformed_name": "company_name",
            }
        )
        .drop_duplicates(
            subset=[
                "central_index_key",
                "name_change_date",
                "company_name_old",
                "company_name",
            ]
        )
    )
    return name_changelog


@dg.asset(
    io_manager_key="pudl_io_manager",
    group_name="core_sec10k",
)
def core_sec10k__quarterly_exhibit_21_company_ownership(
    raw_sec10k__exhibit_21_company_ownership: pd.DataFrame,
) -> pd.DataFrame:
    """Standardize the contents of the SEC 10-K exhibit 21 company ownership table."""
    df = raw_sec10k__exhibit_21_company_ownership.rename(
        columns={
            "sec10k_filename": "filename_sec10k",
            "subsidiary": "subsidiary_company_name",
            "location": "subsidiary_company_location",
        }
    ).assign(
        filename_sec10k=lambda x: _simplify_filename_sec10k(x["filename_sec10k"]),
        fraction_owned=lambda x: _compute_fraction_owned(x["ownership_percentage"]),
        subsidiary_company_location=lambda x: _standardize_location(
            x["subsidiary_company_location"]
        ),
    )
    df["subsidiary_company_name"] = _remove_bad_subsidiary_names(
        df["subsidiary_company_name"].str.strip().str.lower()
    )
    # a record is not meaningful if the subsidiary name is null
    df = df.dropna(subset=["subsidiary_company_name"])
    # construct the CIK of the parent company in order to
    # create subsidiary_company_id_sec10k
    df.loc[:, "parent_company_central_index_key"] = _extract_filer_cik_from_filename(
        df["filename_sec10k"]
    )
    # sometimes there are subsidiaries with the same name but different
    # locations of incorporation listed in the same ex. 21, so include
    # location in the ID
    df.loc[:, "subsidiary_company_id_sec10k"] = (
        df["parent_company_central_index_key"]
        + "_"
        + df["subsidiary_company_name"]
        + "_"
        + df["subsidiary_company_location"].fillna("")
    )
    df = df.drop(columns="parent_company_central_index_key")
    # filename_sec10k and subsidiary_company_id_sec10k should be a primary key
    # for this table, however there are many instances where the model erroneously
    # created two different records for the same subsidiary and location in
    # one Ex. 21 attachment. Often, the fraction_owned field is null for one
    # record and not the other. Keep only one of these records for each
    # subsidiary_company_id_sec10k and filename_sec10k
    n_dupes = len(
        df[df.duplicated(subset=["filename_sec10k", "subsidiary_company_id_sec10k"])]
    )
    logger.info(
        f"""There are {n_dupes} / {len(df)} ({n_dupes / len(df)}) """
        f"""duplicate filename_sec10k and subsidiary_company_id_sec10k records """
        f"""from the extracted Ex. 21 ownership data which are dropped."""
    )
    df = (
        df.sort_values(by=["fraction_owned"])
        .groupby(["filename_sec10k", "subsidiary_company_id_sec10k"])
        .first()
        .reset_index()
    )

    return df


@dg.asset(
    io_manager_key="pudl_io_manager",
    group_name="core_sec10k",
)
def core_sec10k__assn_sec10k_filers_and_eia_utilities(
    raw_sec10k__parents_and_subsidiaries: pd.DataFrame,
) -> pd.DataFrame:
    """Create an association table between SEC 10-K companies and EIA utilities.

    We are retroactively extracting this simple association table (crosswalk) from the
    un-normalized parent/subsidiary relationships handed off from the SEC process
    upstream so it is available as an easy to understand core table.

    Because we accept only the single, highest probability match from the Splink record
    linkage process, we expect a 1-to-1 relationship between these IDs. Note that most
    companies in this table have no CIK because most records come from the unstructured
    Exhibit 21 data, and most that do have a CIK have no EIA Utility ID because most
    companies are not utilities, and most utilities are still unmatched.
    """
    sec_eia_assn = (
        raw_sec10k__parents_and_subsidiaries.loc[
            :, ["central_index_key", "utility_id_eia"]
        ]
        .dropna()
        .drop_duplicates(subset=["central_index_key", "utility_id_eia"])
    )
    return sec_eia_assn


@dg.asset(io_manager_key="pudl_io_manager", group_name="core_sec10k")
def core_sec10k__assn_exhibit_21_subsidiaries_and_filers(
    core_sec10k__quarterly_filings,
    core_sec10k__quarterly_company_information,
    core_sec10k__quarterly_exhibit_21_company_ownership,
) -> pd.DataFrame:
    """Match Ex. 21 subsidiaries to SEC 10k filing companies.

    Create a table associating ``subsidiary_company_id_sec10k`` to that subsidiary
    company's ``central_index_key`` if that subsidiary also files its own 10-K filing.
    The association is created by matching subsidiary companies reported in
    ``core_sec10k__quarterly_exhibit_21_company_ownership`` to their own filing
    information in ``core_sec10k__quarterly_company_information``, which contains
    attributes reported in the main body of a 10-K filing.  To infer the match we merge
    the Ex.21 subsidiaries to 10k filers on company name. If there are multiple matches
    with the same company name we choose the pair with the most overlap in location of
    incorporation and nearest report years.
    """
    filer_info_df = core_sec10k__quarterly_company_information.merge(
        core_sec10k__quarterly_filings[["filename_sec10k", "report_date"]],
        how="left",
        on="filename_sec10k",
        validate="many_to_one",
    )
    ownership_df = core_sec10k__quarterly_exhibit_21_company_ownership.merge(
        core_sec10k__quarterly_filings[["filename_sec10k", "report_date"]],
        how="left",
        on="filename_sec10k",
        validate="many_to_one",
    )
    matched_df = _match_ex21_subsidiaries_to_filer_company(
        filer_info_df=filer_info_df,
        ownership_df=ownership_df,
    )
    out_df = (
        matched_df[
            [
                "subsidiary_company_id_sec10k",
                "subsidiary_company_central_index_key",
            ]
        ]
        .dropna(subset="subsidiary_company_central_index_key")
        .drop_duplicates()
        .rename(columns={"subsidiary_company_central_index_key": "central_index_key"})
    )
    return out_df


@dg.asset(io_manager_key="pudl_io_manager", group_name="core_sec10k")
def core_sec10k__assn_exhibit_21_subsidiaries_and_eia_utilities(
    core_sec10k__assn_sec10k_filers_and_eia_utilities: pd.DataFrame,
    core_eia__entity_utilities: pd.DataFrame,
    core_sec10k__quarterly_exhibit_21_company_ownership: pd.DataFrame,
    core_sec10k__assn_exhibit_21_subsidiaries_and_filers: pd.DataFrame,
) -> pd.DataFrame:
    """An association table between Exhibit 21 subsidiaries and EIA utilities.

    Take the EIA utilities that haven't been matched to a filer company
    and merge them by company name onto the Ex. 21 subsidiaries.
    """
    unmatched_eia_utils_df = core_eia__entity_utilities[
        ~core_eia__entity_utilities["utility_id_eia"].isin(
            core_sec10k__assn_sec10k_filers_and_eia_utilities["utility_id_eia"].unique()
        )
    ]
    unmatched_eia_utils_df["utility_name_eia"] = _standardize_company_name(
        unmatched_eia_utils_df["utility_name_eia"]
    )
    unmatched_eia_utils_df = unmatched_eia_utils_df.drop_duplicates(
        subset=["utility_name_eia"]
    )
    # if a subsidiary is already matched to an SEC filer then
    # it would have gone through the record linkage process to be
    # matched to an EIA utility
    unmatched_subs_df = core_sec10k__quarterly_exhibit_21_company_ownership[
        ~core_sec10k__quarterly_exhibit_21_company_ownership[
            "subsidiary_company_id_sec10k"
        ].isin(
            core_sec10k__assn_exhibit_21_subsidiaries_and_filers[
                "subsidiary_company_id_sec10k"
            ].unique()
        )
    ][["subsidiary_company_name", "subsidiary_company_id_sec10k"]].drop_duplicates()
    unmatched_subs_df["subsidiary_company_name"] = _standardize_company_name(
        unmatched_subs_df["subsidiary_company_name"]
    )
    out_df = unmatched_subs_df.merge(
        unmatched_eia_utils_df[["utility_id_eia", "utility_name_eia"]],
        how="left",
        left_on="subsidiary_company_name",
        right_on="utility_name_eia",
        validate="many_to_many",
    ).dropna(subset="utility_id_eia")[
        ["subsidiary_company_id_sec10k", "utility_id_eia"]
    ]
    return out_df
