"""Rehsaping and data cleaning operations for the SEC 10-K data source."""

import pandas as pd
from dagster import AssetOut, Output, asset, multi_asset

from pudl import logging_helpers

logger = logging_helpers.get_logger(__name__)


@multi_asset(
    outs={
        "_core_sec10k__quarterly_company_information": AssetOut(
            group_name="core_sec10k",
        ),
        "_core_sec10k__changelog_company_name": AssetOut(
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
    # Strip erroneous "]" characters from company information fact names (the keys in
    # the key-value pairs). This has to be done before the table is re-shaped.
    df = raw_sec10k__quarterly_company_information.assign(
        company_information_fact_name=lambda x: x.company_information_fact_name.str.lstrip(
            "]"
        ),
        filename_sec10k=lambda x: x.filename_sec10k.str.removeprefix("edgar/data/")
        .str.removesuffix(".txt")
        .astype("string"),
    )

    def pivot_info_block(df: pd.DataFrame, block: str) -> pd.DataFrame:
        """Extract distinct blocks of company information for separate processing."""
        pivoted = (
            df.loc[df["company_information_block"] == block]
            .pivot(
                values="company_information_fact_value",
                index=[
                    "filename_sec10k",
                    "filer_count",
                    "report_date",
                    "company_information_block_count",
                ],
                columns="company_information_fact_name",
            )
            .convert_dtypes()
        )
        pivoted.columns.name = None
        return pivoted

    business_address = pivot_info_block(df, "business_address").rename(
        columns={"business_phone": "phone"}
    )
    company_data = pivot_info_block(df, "company_data").drop(
        columns=["organization_name"]
    )
    filing_values = pivot_info_block(df, "filing_values")
    mail_address = pivot_info_block(df, "mail_address")
    former_company = pivot_info_block(df, "former_company")

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
        .drop(["company_information_block_count", "report_date"], axis="columns")
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
        Output(
            output_name="_core_sec10k__quarterly_company_information",
            value=company_info,
        ),
        Output(
            output_name="_core_sec10k__changelog_company_name",
            value=name_changes,
        ),
    )


@asset(
    # io_manager_key="pudl_io_manager",
    io_manager_key="parquet_io_manager",
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

    # Split out the name and code for standard industrial classifications based on the
    # most common reporting pattern
    clean_info[["industry_name_sic", "industry_id_sic"]] = clean_info[
        "standard_industrial_classification"
    ].str.extract(r"^(.+)\[(\d{4})\]$")
    # Fill in NA values in the industry_id_sic column with the value from the
    # standard_industrial_classification column if and only if the
    # standard_industrial_classification column is a 4-digit number.
    clean_info["industry_id_sic"] = clean_info["industry_id_sic"].fillna(
        clean_info["standard_industrial_classification"].where(
            clean_info["standard_industrial_classification"].str.match(r"\d{4}"), pd.NA
        )
    )
    clean_info = clean_info.drop(columns=["standard_industrial_classification"])
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

    # Ensure that the EIN/TIN is valid and standardize formatting
    # Should be a 9-digit number formatted NN-NNNNNNN
    # Replace any non-digit characters in the TIN with the empty string
    clean_info["taxpayer_id_irs"] = clean_info["taxpayer_id_irs"].str.replace(
        r"[^\d]", "", regex=True
    )
    not_nine_digits = ~clean_info["taxpayer_id_irs"].str.match(r"^\d{9}$")
    logger.info(f"Nulling {sum(not_nine_digits.dropna())} invalid TINs.")
    assert sum(not_nine_digits.dropna()) < 543
    nine_zeroes = clean_info["taxpayer_id_irs"] == "000000000"
    logger.info(f"Nulling {sum(nine_zeroes.dropna())} all-zero TINs.")
    assert sum(nine_zeroes.dropna()) < 25_156
    clean_info.loc[not_nine_digits | nine_zeroes, "taxpayer_id_irs"] = pd.NA
    clean_info["taxpayer_id_irs"] = (
        clean_info["taxpayer_id_irs"].str[:2]
        + "-"
        + clean_info["taxpayer_id_irs"].str[2:]
    )

    # Remove invalid 10-k form types. The only observed bad values are "th"
    invalid_sec10k_type = clean_info["sec10k_type"] == "th"
    assert sum(invalid_sec10k_type.dropna()) < 11
    clean_info.loc[invalid_sec10k_type, "sec10k_type"] = pd.NA

    return clean_info


@asset(
    # io_manager_key="pudl_io_manager",
    io_manager_key="parquet_io_manager",
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
    )
    # Drop records where there's no name change recorded
    name_changelog = name_changelog.loc[
        name_changelog["company_name_old"] != name_changelog["company_name_new"]
    ]
    return name_changelog
