"""Implement utilities for working with data produced in the pudl modelling repo."""

import pandas as pd
from dagster import AssetIn, asset

from pudl import logging_helpers

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
    io_manager_key="pudl_io_manager",
    group_name="pudl_models",
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
    group_name="pudl_models",
    ins={"raw_df": AssetIn("raw_sec10k__quarterly_company_information")},
)
def core_sec10k__quarterly_company_information(raw_df: pd.DataFrame) -> pd.DataFrame:
    """Company information extracted from SEC10k filings."""
    # Strip erroneous "]" characters
    raw_df["company_information_fact_name"] = raw_df[
        "company_information_fact_name"
    ].str.strip("]")
    raw_df["company_information_block"] = pd.Categorical(
        raw_df["company_information_block"],
        [
            "business_address",
            "mail_address",
            "company_data",
            "filing_values",
            "former_company",
        ],
    )
    df = raw_df.sort_values("company_information_block").pivot_table(
        values="company_information_fact_value",
        index=["filename_sec10k", "report_date"],
        columns="company_information_fact_name",
        aggfunc="first",
    )
    df.columns.name = None
    df = df.reset_index()
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
            "irs_number": "company_id_irs",
            "former_conformed_name": "company_name_former",
            "form_type": "sec10k_version",
            "standard_industrial_classification": "industry_id_sic",
        }
    )

    return df


@asset(
    io_manager_key="pudl_io_manager",
    group_name="pudl_models",
    ins={
        "core_df": AssetIn("core_sec10k__quarterly_company_information"),
        "eia_utils_df": AssetIn("core_eia__entity_utilities"),
    },
)
def out_sec10k__quarterly_company_information(
    core_df: pd.DataFrame, eia_utils_df: pd.DataFrame
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
    out_df = core_df.merge(matched_df, how="left", on="central_index_key")
    # merge utility name on
    out_df = out_df.merge(eia_utils_df, how="left", on="utility_id_eia")
    return out_df


@asset(
    io_manager_key="pudl_io_manager",
    group_name="pudl_models",
    ins={
        "core_df": AssetIn("core_sec10k__quarterly_company_information"),
    },
)
def core_sec10k__changelog_company_name(
    core_df: pd.DataFrame,
) -> pd.DataFrame:
    """Changes in SEC company names and the date of change as reported in 10k filings."""
    changelog_df = core_df[
        [
            "central_index_key",
            "report_date",
            "company_name",
            "name_change_date",
            "company_name_former",
        ]
    ]
    changelog_df = changelog_df[
        (~changelog_df["name_change_date"].isnull())
        | (~changelog_df["company_name_former"].isnull())
    ]
    return changelog_df


@asset(
    io_manager_key="pudl_io_manager",
    group_name="pudl_models",
    ins={
        "filings_df": AssetIn("core_sec10k__quarterly_filings"),
    },
)
def core_sec10k__quarterly_exhibit_21_company_ownership(
    filings_df: pd.DataFrame,
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
        filings_df[
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
        df["parent_central_index_key"]
        + "_"
        + df["subsidiary_company_name"]
        + "_"
        + df["subsidiary_company_location"].fillna("")
    )

    return df


@asset(
    io_manager_key="pudl_io_manager",
    group_name="pudl_models",
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
    group_name="pudl_models",
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
            "irs_number": "company_id_irs",
            "parent_company_cik": "parent_company_central_index_key",
            "files_10k": "files_sec10k",
            "date_of_name_change": "name_change_date",
        }
    )

    # Convert ownership percentage
    df["fraction_owned"] = _compute_fraction_owned(df.ownership_percentage)

    # Split standard industrial classification into ID and description columns
    df[["industry_description_sic", "industry_id_sic"]] = df[
        "standard_industrial_classification"
    ].str.extract(r"(.+)\[(\d{4})\]")
    df["industry_id_sic"] = df["industry_id_sic"].astype("string")
    # Some utilities harvested from EIA 861 data that don't show up in our entity
    # tables. These didn't end up improving coverage, and so will be removed upstream.
    # Hack for now is to just drop them so the FK constraint is respected.
    # See https://github.com/catalyst-cooperative/pudl/issues/4050
    bad_utility_ids = [
        3579,  # Cirro Group, Inc. in Texas
    ]
    df = df[~df.utility_id_eia.isin(bad_utility_ids)]

    return df
