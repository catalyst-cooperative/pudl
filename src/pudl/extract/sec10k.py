"""Produce raw SEC 10-K assets."""

import pandas as pd
from dagster import asset


def _load_table_from_gcs(table_name: str) -> pd.DataFrame:
    return pd.read_parquet(f"gs://model-outputs.catalyst.coop/sec10k/{table_name}")


def _year_quarter_to_date(year_quarter: pd.Series) -> pd.Series:
    """Convert a year quarter in the format '2024q1' to date type."""
    return pd.PeriodIndex(year_quarter, freq="Q").to_timestamp()


@asset(group_name="raw_sec10k")
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
    # Construct a full date from year quarters
    df["report_date"] = _year_quarter_to_date(df.year_quarter)

    return df
