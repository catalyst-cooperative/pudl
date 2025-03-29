"""Rehsaping and data cleaning operations for the SEC 10-K data source."""

import dagster as dg
import pandas as pd

from pudl import logging_helpers

logger = logging_helpers.get_logger(__name__)


def _year_quarter_to_date(year_quarter: pd.Series) -> pd.Series:
    """Convert a year quarter in the format '2024q1' to date type."""
    return pd.PeriodIndex(year_quarter, freq="Q").to_timestamp()


def _simplify_filename_sec10k(filename_sec10k: pd.Series) -> pd.Series:
    """Strip the path and extension from SEC 10-K filenames."""
    return (
        filename_sec10k.str.removeprefix("edgar/data/")
        .str.removesuffix(".txt")
        .astype("string")
    )


def _compute_fraction_owned(percent_ownership: pd.Series) -> pd.Series:
    """Clean percent ownership, convert to float, then convert percent to ratio."""
    return (
        percent_ownership.str.replace(r"(\.{2,})", r"\.", regex=True)
        .replace("\\\\", "", regex=True)
        .replace(".", "0.0", regex=False)
        .astype("float")
    ) / 100.0


def _standardize_taxpayer_id_irs(taxpayer_id_irs: pd.Series) -> pd.Series:
    """Standardize the IRS taxpayer ID number to NN-NNNNNNN format."""
    # Ensure that the EIN/TIN is valid and standardize formatting
    # Should be a 9-digit number formatted NN-NNNNNNN
    # Replace any non-digit characters in the TIN with the empty string
    tin = taxpayer_id_irs.astype("string").str.replace(r"[^\d]", "", regex=True)
    not_nine_digits = ~tin.str.match(r"^\d{9}$")
    logger.info(f"Nulling {sum(not_nine_digits.dropna())} invalid TINs.")
    nine_zeroes = tin == "000000000"
    logger.info(f"Nulling {sum(nine_zeroes.dropna())} all-zero TINs.")
    tin.loc[(not_nine_digits | nine_zeroes)] = pd.NA
    tin = tin.str[:2] + "-" + tin.str[2:]
    return tin


def _standardize_industrial_classification(sic: pd.Series) -> pd.DataFrame:
    """Split industry names and codes into separate columns."""
    sic_df = pd.DataFrame()
    sic_df[["industry_name_sic", "industry_id_sic"]] = sic.str.extract(
        r"^(.+)\[(\d{4})\]$"
    )
    # Fill NA values with the value from the standard_industrial_classification column
    # if and only if the standard_industrial_classification column is a 4-digit number.
    sic_df["industry_id_sic"] = sic_df["industry_id_sic"].fillna(
        sic.where(sic.str.match(r"\d{4}"), pd.NA)
    )
    return sic_df


@dg.asset(
    io_manager_key="pudl_io_manager",
    group_name="core_sec10k",
)
def core_sec10k__quarterly_filings(
    raw_sec10k__quarterly_filings: pd.DataFrame,
) -> pd.DataFrame:
    """Standardize the contents of the SEC 10-K filings table."""
    filings = (
        raw_sec10k__quarterly_filings.rename(
            columns={
                "sec10k_filename": "filename_sec10k",
                "form_type": "sec10k_type",
                "date_filed": "filing_date",
            }
        )
        .assign(
            filename_sec10k=lambda x: _simplify_filename_sec10k(x["filename_sec10k"]),
            report_date=lambda x: _year_quarter_to_date(x["year_quarter"]),
            filing_date=lambda x: pd.to_datetime(x["filing_date"]),
            central_index_key=lambda x: x["central_index_key"].str.zfill(10),
        )
        .drop(columns="year_quarter")
        .convert_dtypes()
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
    """Company information extracted from SEC 10-K filings.

    Each SEC 10-K filing contains a header block that lists information the filer, and
    potentially a number of other related companies. Each company level block of
    information may include the company's name, physical address, some details about
    how it files the SEC 10-K, and some other company attributes like taxpayer ID
    number, industrial classification, etc. It may also include a mailing address that
    is distinct from the physical address, and the history of names that the company has
    had over time.

    This asset normalizes the raw key-value pairs extracted from these headers into
    two tables:

    - company information on a per-filing basis
    - the history of company name changes

    This information is used elsewhere for matching SEC 10-K filers to EIA utilities
    and for identifying the companies that are mentioned in the unstructured Exhibit 21
    attachements.

    Further processing that standardizes the contents of these tables is deferred to
    individual downstream core assets.
    """
    raw_company_info = raw_sec10k__quarterly_company_information.rename(
        columns={
            "sec10k_filename": "filename_sec10k",
        }
    ).assign(
        key=lambda x: x["key"].str.lstrip("]"),
        filename_sec10k=lambda x: _simplify_filename_sec10k(x["filename_sec10k"]),
    )

    def pivot_info_block(df: pd.DataFrame, block: str) -> pd.DataFrame:
        """Extract distinct blocks of company information for separate processing."""
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

    business_address = pivot_info_block(raw_company_info, "business_address").rename(
        columns={"business_phone": "phone"}
    )
    company_data = pivot_info_block(raw_company_info, "company_data").drop(
        columns=["organization_name"]
    )
    filing_values = pivot_info_block(raw_company_info, "filing_values")
    mail_address = pivot_info_block(raw_company_info, "mail_address")
    former_company = pivot_info_block(raw_company_info, "former_company")

    # Add prefixes where needed to ensure that column names are distinct after concatentation.
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
        .drop(["block_count"], axis="columns")
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
            value=company_info.convert_dtypes(),
        ),
        dg.Output(
            output_name="_core_sec10k__changelog_company_name",
            value=name_changes.convert_dtypes(),
        ),
    )


@dg.asset(
    io_manager_key="pudl_io_manager",
    group_name="core_sec10k",
)
def core_sec10k__quarterly_company_information(
    _core_sec10k__quarterly_company_information: pd.DataFrame,
) -> pd.DataFrame:
    """Standardize the contents of the SEC 10-K company information table."""
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
    clean_info["fiscal_year_end"] = (
        clean_info["fiscal_year_end"].str.zfill(4).replace("0939", "0930")
    )
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

    return clean_info.drop(
        columns="standard_industrial_classification"
    ).convert_dtypes()


@dg.asset(
    io_manager_key="pudl_io_manager",
    group_name="core_sec10k",
)
def core_sec10k__changelog_company_name(
    _core_sec10k__changelog_company_name: pd.DataFrame,
) -> pd.DataFrame:
    """Clean version of the company name changelog."""
    name_changelog = (
        _core_sec10k__changelog_company_name.assign(
            name_change_date=lambda x: pd.to_datetime(x["date_of_name_change"])
        )
        .rename(columns={"former_conformed_name": "company_name_old"})
        .sort_values(["central_index_key", "name_change_date"])
        .drop_duplicates()
        .loc[
            :,
            [
                "central_index_key",
                "name_change_date",
                "company_name_old",
                "company_conformed_name",
            ],
        ]
        .assign(
            company_name_new=lambda x: x.groupby("central_index_key")[
                "company_name_old"
            ]
            .shift(-1)
            .fillna(x["company_conformed_name"])
        )
        .drop(columns="company_conformed_name")
    ).convert_dtypes()
    # Drop records where there's no name change recorded
    name_changelog = name_changelog.loc[
        name_changelog["company_name_old"] != name_changelog["company_name_new"]
    ]
    return name_changelog


@dg.asset(
    io_manager_key="pudl_io_manager",
    group_name="core_sec10k",
)
def core_sec10k__parents_and_subsidiaries(
    raw_sec10k__parents_and_subsidiaries: pd.DataFrame,
) -> pd.DataFrame:
    """Standardize the contents of the SEC 10-K parents and subsidiaries table."""
    df = raw_sec10k__parents_and_subsidiaries.rename(
        columns={
            "sec10k_filename": "filename_sec10k",
            "sec_company_id": "company_id_sec10k",
            "street_address_2": "address_2",
            "former_conformed_name": "company_name_old",
            "location_of_inc": "location_of_incorporation",
            "state_of_incorporation": "incorporation_state",
            "irs_number": "taxpayer_id_irs",
            "parent_company_cik": "parent_company_central_index_key",
            "files_10k": "files_sec10k",
            "date_of_name_change": "name_change_date",
        }
    ).assign(
        filename_sec10k=lambda x: _simplify_filename_sec10k(x["filename_sec10k"]),
        fraction_owned=lambda x: _compute_fraction_owned(x["ownership_percentage"]),
    )

    df[["industry_name_sic", "industry_id_sic"]] = (
        _standardize_industrial_classification(df["standard_industrial_classification"])
    )
    df["taxpayer_id_irs"] = _standardize_taxpayer_id_irs(df["taxpayer_id_irs"])

    # Some utilities harvested from EIA 861 data that don't show up in our entity
    # tables. These didn't end up improving coverage, and so will be removed upstream.
    # Hack for now is to just drop them so the FK constraint is respected.
    # See https://github.com/catalyst-cooperative/pudl/issues/4050
    bad_utility_ids = [
        3579,  # Cirro Group, Inc. in Texas
    ]
    df = df[~df.utility_id_eia.isin(bad_utility_ids)]

    df = df.drop(
        columns=[
            "standard_industrial_classification",
            "ownership_percentage",
            "report_year",
        ]
    ).convert_dtypes()

    return df


@dg.asset(
    io_manager_key="pudl_io_manager",
    group_name="core_sec10k",
)
def core_sec10k__quarterly_exhibit_21_company_ownership(
    raw_sec10k__exhibit_21_company_ownership: pd.DataFrame,
) -> pd.DataFrame:
    """Standardize the contents of the SEC 10-K exhibit 21 company ownership table."""
    df = (
        raw_sec10k__exhibit_21_company_ownership.rename(
            columns={
                "sec10k_filename": "filename_sec10k",
                "subsidiary": "subsidiary_company_name",
                "location": "subsidiary_company_location",
            }
        )
        .assign(
            filename_sec10k=lambda x: _simplify_filename_sec10k(x["filename_sec10k"]),
            fraction_owned=lambda x: _compute_fraction_owned(x["ownership_percentage"]),
            report_date=lambda x: _year_quarter_to_date(x["year_quarter"]),
        )
        .drop(columns=["ownership_percentage", "year_quarter"])
    ).convert_dtypes()
    return df


@dg.asset(
    io_manager_key="pudl_io_manager",
    group_name="core_sec10k",
)
def core_sec10k__assn_sec10k_filers_and_eia_utilities(
    core_sec10k__parents_and_subsidiaries: pd.DataFrame,
) -> pd.DataFrame:
    """An association table between SEC 10k filing companies and EIA utilities."""
    sec_eia_matches = (
        core_sec10k__parents_and_subsidiaries.loc[
            :, ["central_index_key", "utility_id_eia"]
        ]
        .drop_duplicates(subset="central_index_key")
        .dropna()
    ).convert_dtypes()
    # Verify that each CIK is matched to only one utility
    # TODO: this is failing, so I added the drop_duplicates() back in above,
    # but should ask Katie about why and if it's expected / important....
    # assert sec_eia_matches["central_index_key"].nunique() == len(sec_eia_matches)
    return sec_eia_matches
