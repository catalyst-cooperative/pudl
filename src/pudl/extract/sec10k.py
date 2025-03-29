"""Produce raw SEC 10-K assets."""

import dagster as dg
import pandas as pd


def _load_table_from_gcs(table_name: str) -> pd.DataFrame:
    return pd.read_parquet(f"gs://model-outputs.catalyst.coop/sec10k/{table_name}")


@dg.asset(group_name="raw_sec10k")
def raw_sec10k__quarterly_filings() -> pd.DataFrame:
    """Metadata on all 10k filings submitted to SEC."""
    return _load_table_from_gcs("core_sec10k__filings")


@dg.asset(group_name="raw_sec10k")
def raw_sec10k__quarterly_company_information() -> pd.DataFrame:
    """Raw company information harvested from headers of SEC10k filings."""
    return _load_table_from_gcs("core_sec10k__company_information")


@dg.asset(group_name="raw_sec10k")
def raw_sec10k__parents_and_subsidiaries() -> pd.DataFrame:
    """Raw parent and subsidiary relationships extracted from SEC 10k filings."""
    return _load_table_from_gcs("out_sec10k__parents_and_subsidiaries")


@dg.asset(group_name="raw_sec10k")
def raw_sec10k__exhibit_21_company_ownership() -> pd.DataFrame:
    """Raw company ownership information extracted from sec10k exhibit 21 attachments."""
    return _load_table_from_gcs("core_sec10k__exhibit_21_company_ownership")
