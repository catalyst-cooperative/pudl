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
    df["filer_cik"] = df["filename_sec10k"].str.split("/").str[2].str.zfill(10)
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
def core_sec10k__quarterly_exhibit_21_company_ownership() -> pd.DataFrame:
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

    # Get date from year quarters
    df["report_date"] = _year_quarter_to_date(df.year_quarter)

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


@asset(
    io_manager_key="pudl_io_manager",
    group_name="sec10k",
)
def out_sec10k__parents_and_subsidiaries() -> pd.DataFrame:
    """Denormalized output table with sec10k info and company ownership linked to EIA."""
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
