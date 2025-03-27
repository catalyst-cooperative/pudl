"""Implement utilities for working with data produced in the pudl modelling repo."""

import pandas as pd
from dagster import asset

from pudl import logging_helpers
from pudl.extract.sec10k import _load_table_from_gcs, _year_quarter_to_date

logger = logging_helpers.get_logger(__name__)


def _compute_fraction_owned(percent_ownership: pd.Series) -> pd.Series:
    """Clean percent ownership, convert to float, then convert percent to ratio."""
    return (
        percent_ownership.str.replace(r"(\.{2,})", r"\.", regex=True)
        .replace("\\\\", "", regex=True)
        .replace(".", "0.0", regex=False)
        .astype("float")
    ) / 100.0


@asset(
    # io_manager_key="pudl_io_manager",
    group_name="sec10k",
)
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
    # io_manager_key="pudl_io_manager",
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
    # io_manager_key="pudl_io_manager",
    group_name="sec10k",
)
def core_sec10k__quarterly_filings() -> pd.DataFrame:
    """Metadata on all 10k filings submitted to SEC."""
    df = _load_table_from_gcs("core_sec10k__filings")
    df = df.rename(
        columns={
            "sec10k_filename": "filename_sec10k",
            "form_type": "sec10k_type",
            "date_filed": "filing_date",
        }
    )

    # Get date from year quarters
    df["report_date"] = _year_quarter_to_date(df.year_quarter)
    df["central_index_key"] = df["central_index_key"].str.zfill(10)

    return df


@asset(
    # io_manager_key="pudl_io_manager",
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
        & (df["taxpayer_id_irs"].str.isnumeric()),
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
