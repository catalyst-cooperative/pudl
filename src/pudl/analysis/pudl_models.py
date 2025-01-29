"""Implement utilities for working with data produced in the pudl modelling repo."""

import pandas as pd
from dagster import asset


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


@asset(
    io_manager_key="parquet_io_manager",
    group_name="pudl_models",
)
def core_sec10k__company_information() -> pd.DataFrame:
    """Basic company information extracted from SEC10k filings."""
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

    return df


@asset(
    io_manager_key="parquet_io_manager",
    group_name="pudl_models",
)
def core_sec10k__exhibit_21_company_ownership() -> pd.DataFrame:
    """Company ownership information extracted from sec10k exhibit 21 attachments."""
    df = _load_table_from_gcs("core_sec10k__exhibit_21_company_ownership")
    df = df.rename(
        columns={
            "sec10k_filename": "filename_sec10k",
            "subsidiary": "subsidiary_company_name",
            "location": "subsidiary_location",
        }
    )

    # Convert ownership percentage
    df["fraction_owned"] = _compute_fraction_owned(df.ownership_percentage)

    return df


@asset(
    io_manager_key="parquet_io_manager",
    group_name="pudl_models",
)
def core_sec10k__filings() -> pd.DataFrame:
    """Metadata on all 10k filings submitted to SEC."""
    df = _load_table_from_gcs("core_sec10k__filings")
    df = df.rename(
        columns={
            "sec10k_filename": "filename_sec10k",
            "form_type": "sec10k_version",
        }
    )

    return df


@asset(
    io_manager_key="parquet_io_manager",
    group_name="pudl_models",
)
def out_sec10k__parents_and_subsidiaries() -> pd.DataFrame:
    """Denormalized output table with sec10k info and company ownership linked to EIA."""
    df = _load_table_from_gcs("out_sec10k__parents_and_subsidiaries")
    df = df.rename(
        columns={
            "sec10k_filename": "filename_sec10k",
            "sec_company_id": "company_id_sec",
            "street_address_2": "address_2",
            "former_conformed_name": "company_name_former",
            "location_of_inc": "location_of_incorporation",
            "irs_number": "company_id_irs",
            "parent_company_cik": "parent_company_central_index_key",
        }
    )

    # Convert ownership percentage
    df["fraction_owned"] = _compute_fraction_owned(df.ownership_percentage)

    return df
