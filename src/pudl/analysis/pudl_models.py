"""Implement utilities for working with data produced in the pudl modelling repo."""

import pandas as pd
from dagster import asset

from pudl import logging_helpers
from pudl.helpers import convert_cols_dtypes

logger = logging_helpers.get_logger(__name__)


def _load_table_from_gcs(table_name: str) -> pd.DataFrame:
    return pd.read_parquet(f"gs://model-outputs.catalyst.coop/sec10k/{table_name}")


def _compute_fraction_owned(percent_ownership: pd.Series) -> pd.Series:
    """Clean percent ownership, convert to float, then convert percent to ratio."""
    return (
        percent_ownership.str.replace(r"(\.{2,})", r"\.", regex=True)
        .replace("\\\\", "", regex=True)
        .replace(".", "0.0", regex=False)
        .astype("float")
    ) / 100.0


def _year_quarter_to_date(year_quarter: pd.Series) -> pd.Series:
    """Convert a year quarter in the format '2024q1' to date type."""
    return pd.PeriodIndex(year_quarter, freq="Q").to_timestamp()


def _get_cik_from_filename(filename_sec10k: pd.Series) -> pd.Series:
    """Get the CIK of the filer from the filename strings."""
    return filename_sec10k.str.split("/").str[2].str.zfill(10)


def match_ex21_subsidiaries_to_filer_company(
    filer_info_df: pd.DataFrame,
    ownership_df: pd.DataFrame,
) -> pd.DataFrame:
    """Match Ex. 21 subsidiaries to filer companies.

    We want to assign CIKs to Ex. 21 subsidiaries if they in turn
    file a 10k. To do this, we merge the Ex. 21 subsidiaries to 10k
    filers on comapny name. If there are multiple matches with the same
    company name we take the company with the most overlap in location of
    incorporation and nearest report years. Then we merge the CIK back onto
    the Ex. 21 df.

    Returns:
        A dataframe of the Ex. 21 subsidiaries with a column for the
        subsidiaries CIK (null if the subsidiary doesn't file).
    """
    filer_info_df = filer_info_df.drop_duplicates(
        subset=[
            "central_index_key",
            "company_name",
            "state_of_incorporation",
            "report_date",
        ]
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
        left_on="company_name",
        right_on="subsidiary_company_name",
        suffixes=("_sec", "_ex21"),
    )
    # split up the location of incorporation on whitespace, creating a column
    # with lists of word tokens
    merged_df.loc[:, "loc_tokens_sec"] = (
        merged_df["state_of_incorporation"].fillna("").str.lower().str.split()
    )
    merged_df.loc[:, "loc_tokens_ex21"] = (
        merged_df["subsidiary_company_location"].fillna("").str.lower().str.split()
    )
    # get the number of words overlapping between location of incorporation tokens
    merged_df["loc_overlap"] = merged_df.apply(
        lambda row: len(set(row["loc_tokens_sec"]) & set(row["loc_tokens_ex21"])),
        axis=1,
    )
    # get the difference in report dates
    merged_df["report_date_diff_days"] = (
        merged_df["report_date_sec"] - merged_df["report_date_ex21"]
    ).dt.days
    merged_df = merged_df.sort_values(
        by=[
            "company_name",
            "subsidiary_company_location",
            "loc_overlap",
            "report_date_diff_days",
        ],
        ascending=[True, True, False, True],
    )
    # Select the row with the highest loc overlap and nearest report dates
    # for each company name and location
    closest_match_df = merged_df.groupby(
        ["company_name", "subsidiary_company_location"], as_index=False
    ).first()
    # TODO: does it work to merge with null values in location or do
    # we have to put filename and report date in the groupby above
    ownership_with_cik_df = ownership_df.merge(
        closest_match_df[
            [
                "company_name",
                "subsidiary_company_location",
                "central_index_key",
            ]
        ],
        how="left",
        left_on=["subsidiary_company_name", "subsidiary_company_location"],
        right_on=["company_name", "subsidiary_company_location"],
    ).rename(columns={"central_index_key": "subsidiary_company_central_index_key"})
    # if a subsidiary doesn't have a CIK and has a null location
    # but its company name was assigned a CIK (with a different location)
    # then assign that CIK to the subsidiary
    ownership_with_cik_df = ownership_with_cik_df.merge(
        closest_match_df[["company_name", "central_index_key"]],
        how="left",
        on="company_name",
    ).rename(columns={"central_index_key": "company_name_merge_cik"})
    ownership_with_cik_df["subsidiary_company_central_index_key"] = (
        ownership_with_cik_df["subsidiary_company_central_index_key"].where(
            ~(ownership_with_cik_df.subsidiary_company_central_index_key.isnull())
            | ~(ownership_with_cik_df.subsidiary_company_location.isnull()),
            ownership_with_cik_df["company_name_merge_cik"],
        )
    )
    ownership_with_cik_df = ownership_with_cik_df.drop(columns="company_name_merge_cik")
    return ownership_with_cik_df


@asset(
    group_name="sec10k",
)
def raw_sec10k__quarterly_company_information() -> pd.DataFrame:
    """Raw company information harvested from headers of SEC10k filings."""
    df = _load_table_from_gcs("core_sec10k__company_information")
    df = df.rename(
        columns={
            "sec10k_filename": "filename_sec10k",
            "block": "company_information_block",
            "block_count": "company_information_block_count",
            "key": "company_information_fact_name",
            "value": "company_information_fact_value",
        }
    )
    # Get date from year quarters
    df["report_date"] = _year_quarter_to_date(df.year_quarter)

    return df


@asset(
    io_manager_key="pudl_io_manager",
    group_name="sec10k",
)
def core_sec10k__quarterly_company_information(
    raw_sec10k__quarterly_company_information: pd.DataFrame,
) -> pd.DataFrame:
    """Company information extracted from SEC10k filings.

    Consolidate company information extracted from key: value blocks
    within the headers of the SEC 10k filings such that there
    is one record of company information per block. One company's
    information may be reported in multiple filings on the same report date.
    However, we only want to keep one of those records per company and report date,
    (``central index key`` and ``report_date`` are a primary key for the table).
    We prioritize keeping records from filings where that company's extracted
    ``central_index_key`` matches the filer's central index key, meaning that
    that company filed the 10k itself.
    """
    # Strip erroneous "]" characters
    raw_sec10k__quarterly_company_information["company_information_fact_name"] = (
        raw_sec10k__quarterly_company_information[
            "company_information_fact_name"
        ].str.lstrip("]")
    )
    df = raw_sec10k__quarterly_company_information.pivot(
        values="company_information_fact_value",
        index=[
            "filename_sec10k",
            "report_date",
            "company_information_block",
            "company_information_block_count",
        ],
        columns="company_information_fact_name",
    )
    df.columns.name = None
    df = df.reset_index()
    # consolidate information extracted from blocks within the header
    # so that there is one record per block
    df = (
        (
            df.groupby(
                ["filename_sec10k", "report_date", "company_information_block_count"]
            ).first()
        )
        .reset_index()
        .drop(columns=["company_information_block", "company_information_block_count"])
        .dropna(subset="central_index_key")
    )
    # we want central_index_key and report_date to be a primary key
    # prioritize records where the filer is the same
    # as the harvested central index key value
    df["filer_cik"] = _get_cik_from_filename(df["filename_sec10k"])
    df["filer_cik_matches_cik"] = df["filer_cik"] == df["central_index_key"]
    df = df.sort_values(by="filer_cik_matches_cik", ascending=False).drop_duplicates(
        subset=["central_index_key", "report_date"], keep="first"
    )
    df = df.drop(columns=["filer_cik", "filer_cik_matches_cik"])
    df = df.rename(
        columns={
            "street_1": "street_address",
            "street_2": "address_2",
            "company_conformed_name": "company_name",
            "date_of_name_change": "name_change_date",
            "zip": "zip_code",
            "business_phone": "phone_number",
            "irs_number": "taxpayer_id_irs",
            "former_conformed_name": "company_name_former",
            "form_type": "sec10k_version",
            "standard_industrial_classification": "industry_id_sic",
            "sec_file_number": "filing_number_sec",
        }
    )
    df["zip_code"] = df["zip_code"].str[:5]
    df["zip_code_4"] = df["zip_code"].str[-4:]
    df["zip_code_4"].where(df["zip_code"].str.len() > 5, None)
    df["name_change_date"] = pd.to_datetime(df["name_change_date"], format="%Y%m%d")
    df["state"] = df["state"].str.upper()
    df["state_of_incorporation"] = df["state_of_incorporation"].str.upper()
    df[["industry_name_sic", "industry_id_sic"]] = df["industry_id_sic"].str.extract(
        r"^(.+)\[(\d{4})\]$"
    )
    df["sec_act"] = df["sec_act"].where(df["sec_act"].isnull(), "1934 act")
    # fiscal year end should conform to MMDD format
    df["fiscal_year_end"] = df["fiscal_year_end"].str.zfill(4)
    df.loc[
        ~df.fiscal_year_end.str.contains(
            r"^(?:(?:0[1-9]|1[0-2])(?:0[1-9]|1\d|2\d|3[01])|(?:0[13-9]|1[0-2])(?:29|30)|(?:0[13578]|1[02])31)$",
            na=False,
        ),
        "fiscal_year_end",
    ] = None
    # make taxpayer ID a 9 digit number with a dash separating the first two digits
    df["taxpayer_id_irs"] = df["taxpayer_id_irs"].str.replace("-", "", regex=False)
    df["taxpayer_id_irs"] = df["taxpayer_id_irs"].where(
        (df["taxpayer_id_irs"].str.len() == 9)
        & (df["taxpayer_id_irs"].str.isnumeric().all()),
        pd.NA,
    )
    df["taxpayer_id_irs"] = (
        df["taxpayer_id_irs"].str[:2] + "-" + df["taxpayer_id_irs"].str[-7:]
    )
    df = convert_cols_dtypes(df, data_source="sec10k")
    df = df.sort_values(by=["central_index_key", "report_date"])

    return df


@asset(
    io_manager_key="pudl_io_manager",
    group_name="sec10k",
)
def out_sec10k__quarterly_company_information(
    core_sec10k__quarterly_company_information: pd.DataFrame,
    core_eia__entity_utilities: pd.DataFrame,
) -> pd.DataFrame:
    """Company information extracted from SEC10k filings and matched to EIA utilities."""
    matched_df = _load_table_from_gcs("out_sec10k__parents_and_subsidiaries")
    matched_df = (
        matched_df[["central_index_key", "utility_id_eia"]]
        .dropna()
        .drop_duplicates(
            subset="central_index_key"
        )  # matches should already be 1-to-1 but drop duplicates to ensure this is true
    )
    out_df = core_sec10k__quarterly_company_information.merge(
        matched_df, how="left", on="central_index_key"
    )
    # merge utility name on
    out_df = out_df.merge(core_eia__entity_utilities, how="left", on="utility_id_eia")
    return out_df


@asset(io_manager_key="pudl_io_manager", group_name="sec10k")
def core_sec10k__assn__sec10k_filers_and_eia_utilities() -> pd.DataFrame:
    """An association table between SEC 10k filing companies and EIA utilities."""
    matched_df = _load_table_from_gcs("out_sec10k__parents_and_subsidiaries")
    matched_df = (
        matched_df[["central_index_key", "utility_id_eia"]]
        .dropna()
        .drop_duplicates(
            subset="central_index_key"
        )  # matches should already be 1-to-1 but drop duplicates to ensure this is true
    )
    return matched_df


@asset(
    io_manager_key="pudl_io_manager",
    group_name="sec10k",
)
def core_sec10k__changelog_company_name(
    core_sec10k__quarterly_company_information: pd.DataFrame,
) -> pd.DataFrame:
    """Changes in SEC company names and the date of change as reported in 10k filings.

    When a company never reported under its former name, create a record
    for that company name and concatenate with the existing names
    to get a log of name changes.
    """
    changelog_df = core_sec10k__quarterly_company_information[
        ["central_index_key", "company_name", "name_change_date", "company_name_former"]
    ].drop_duplicates()
    changelog_df = changelog_df[~changelog_df["company_name"].isnull()]
    # often a company never filed a 10k under its former name
    # create records for these former names and concatenate
    # them with the changed names so that we can have a log
    # of names changes
    former_names_df = (
        changelog_df[["central_index_key", "company_name_former"]]
        .dropna(subset="company_name_former")
        .rename(columns={"company_name_former": "company_name"})
    )
    changelog_df = pd.concat([changelog_df, former_names_df])
    changelog_df = changelog_df.sort_values(
        by=["central_index_key", "name_change_date"], na_position="first"
    )
    changelog_df = changelog_df.drop_duplicates(
        subset=["central_index_key", "company_name"], keep="last"
    )
    changelog_df = changelog_df[
        ["central_index_key", "company_name", "name_change_date"]
    ]
    return changelog_df


@asset(
    io_manager_key="pudl_io_manager",
    group_name="sec10k",
)
def core_sec10k__quarterly_exhibit_21_company_ownership(
    core_sec10k__quarterly_filings: pd.DataFrame,
) -> pd.DataFrame:
    """Company ownership information extracted from sec10k exhibit 21 attachments."""
    df = _load_table_from_gcs("core_sec10k__exhibit_21_company_ownership")
    df = df.rename(
        columns={
            "sec10k_filename": "filename_sec10k",
            "subsidiary": "subsidiary_company_name",
            "location": "subsidiary_company_location",
        }
    )

    # Convert ownership percentage
    df["fraction_owned"] = _compute_fraction_owned(df.ownership_percentage)
    df = df.merge(
        core_sec10k__quarterly_filings[
            [
                "filename_sec10k",
                "central_index_key",
                "company_name",
                "filing_date",
                "report_date",
            ]
        ],
        how="left",
        on="filename_sec10k",
    )
    df = df.rename(
        columns={
            "company_name": "parent_company_name",
            "central_index_key": "parent_company_central_index_key",
        }
    )
    df["parent_company_name"] = df["parent_company_name"].str.lower()
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

    return df


@asset(
    io_manager_key="pudl_io_manager",
    group_name="sec10k",
)
def core_sec10k__quarterly_filings() -> pd.DataFrame:
    """Metadata on all 10k filings submitted to SEC."""
    df = _load_table_from_gcs("core_sec10k__filings")
    df = df.rename(
        columns={
            "sec10k_filename": "filename_sec10k",
            "form_type": "sec10k_version",
            "date_filed": "filing_date",
        }
    )

    # Get date from year quarters
    df["report_date"] = _year_quarter_to_date(df.year_quarter)
    df["central_index_key"] = df["central_index_key"].str.zfill(10)

    return df


@asset(group_name="sec10k")
def core_sec10k__assn__exhibit_21_subsidiaries_and_filers(
    core_sec10k__quarterly_company_information,
    core_sec10k__quarterly_exhibit_21_company_ownership,
) -> pd.DataFrame:
    """Match Ex. 21 subsidiaries to SEC 10k filing companies.

    Create an association between ``subsidiary_company_id_sec10k``
    and ``central_index_key``.
    """
    matched_df = match_ex21_subsidiaries_to_filer_company(
        filer_info_df=core_sec10k__quarterly_company_information,
        ownership_df=core_sec10k__quarterly_exhibit_21_company_ownership,
    )
    # TODO: does this table change over time? include report_date?
    matched_df = (
        matched_df[
            ["subsidiary_company_id_sec10k", "subsidiary_company_central_index_key"]
        ]
        .rename(columns={"subsidiary_company_central_index_key": "central_index_key"})
        .dropna(subset="central_index_key")
    ).drop_duplicates(subset="subsidiary_company_id_sec10k")
    return matched_df


@asset(
    io_manager_key="pudl_io_manager",
    group_name="sec10k",
)
def out_sec10k__parents_and_subsidiaries(
    core_sec10k__quarterly_exhibit_21_company_ownership: pd.DataFrame,
    core_sec10k__quarterly_company_information: pd.DataFrame,
) -> pd.DataFrame:
    """Denormalized output table with Sec10k company attributes and ownership info linked to EIA."""
    # merge parent attributes on
    df = core_sec10k__quarterly_exhibit_21_company_ownership.merge(
        core_sec10k__quarterly_company_information,
        how="left",
        left_on=["parent_company_central_index_key", "report_date"],
        right_on=["central_index_key", "report_date"],
    )

    df = _load_table_from_gcs("out_sec10k__parents_and_subsidiaries")
    df = df.rename(
        columns={
            "sec10k_filename": "filename_sec10k",
            "sec_company_id": "company_id_sec10k",
            "street_address_2": "address_2",
            "former_conformed_name": "company_name_former",
            "location_of_inc": "location_of_incorporation",
            "irs_number": "taxpayer_id_irs",
            "parent_company_cik": "parent_company_central_index_key",
            "files_10k": "files_sec10k",
            "date_of_name_change": "name_change_date",
        }
    )

    # Convert ownership percentage
    df["fraction_owned"] = _compute_fraction_owned(df.ownership_percentage)

    # Split standard industrial classification into ID and description columns
    df[["industry_name_sic", "industry_id_sic"]] = df[
        "standard_industrial_classification"
    ].str.extract(r"^(.+)\[(\d{4})\]$")
    df["industry_id_sic"] = df["industry_id_sic"].astype("string")
    # make taxpayer ID a 9 digit number with a dash separating the first two digits
    df["taxpayer_id_irs"] = df["taxpayer_id_irs"].str.replace("-", "", regex=False)
    df["taxpayer_id_irs"] = df["taxpayer_id_irs"].where(
        (df["taxpayer_id_irs"].str.len() == 9)
        & (df["taxpayer_id_irs"].str.isnumeric().all()),
        pd.NA,
    )
    df["taxpayer_id_irs"] = (
        df["taxpayer_id_irs"].str[:2] + "-" + df["taxpayer_id_irs"].str[-7:]
    )
    # Some utilities harvested from EIA 861 data that don't show up in our entity
    # tables. These didn't end up improving coverage, and so will be removed upstream.
    # Hack for now is to just drop them so the FK constraint is respected.
    # See https://github.com/catalyst-cooperative/pudl/issues/4050
    bad_utility_ids = [
        3579,  # Cirro Group, Inc. in Texas
    ]
    df = df[~df.utility_id_eia.isin(bad_utility_ids)]

    return df
