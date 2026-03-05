"""Transform EIA Form 191 monthly underground natural gas storage data."""

import pandas as pd
from dagster import asset

import pudl.helpers
from pudl.logging_helpers import get_logger

logger = get_logger(__name__)


@asset
def core_eia191__monthly_gas_storage(
    raw_eia191__data: pd.DataFrame,
) -> pd.DataFrame:
    """Transform raw EIA-191 monthly underground natural gas storage data.

    One row per storage reservoir per month, identified by the EIA-assigned
    reservoir id and report_date.

    Transformations include:

    * Drop ``report_year`` (always equal to ``year``; extractor artifact).
    * Combine ``year`` and ``month`` into a single ``report_date`` column.
    * Rename ``base_gas`` to ``base_gas_mcf`` to make the Mcf unit explicit.
    * Convert string columns to nullable ``StringDtype``.
    * Convert integer columns to nullable ``Int64Dtype``.
    * Pass ``base_gas_mcf``, ``working_gas_capacity_mcf``, and
      ``total_field_capacity_mcf`` through as reported. These are operator
      self-reports under a loose EIA definition and are not additively
      consistent: ``total`` does not reliably equal ``working + base`` (~23%
      of rows differ). Do not assume additivity.

    Args:
        raw_eia191__data: Concatenated raw CSV data from the PUDL EIA-191
            Zenodo archive (RP8 monthly dataset, 2014–present).

    Returns:
        Cleaned monthly gas storage DataFrame with standardized dtypes and a
        combined report_date column.
    """
    df = raw_eia191__data.copy()

    # report_year is always equal to year — added by the extractor as a
    # processing artifact. Drop it before converting year+month to report_date.
    df = df.drop(columns=["report_year"])

    df = pudl.helpers.convert_to_date(
        df, date_col="report_date", year_col="year", month_col="month"
    )

    # EIA-191 instructions state all volumes are reported in thousand cubic feet (Mcf).
    df = df.rename(columns={"base_gas": "base_gas_mcf"})

    # Convert string columns to nullable StringDtype
    string_cols = [
        "id",
        "report_state",
        "company_name",
        "field_name",
        "reservoir_name",
        "field_type",
        "county_name",
        "status",
        "region",
    ]
    for col in string_cols:
        df[col] = df[col].astype(pd.StringDtype())

    # Convert integer columns to nullable Int64Dtype
    int_cols = ["gas_field_code", "reservoir_code"]
    for col in int_cols:
        df[col] = df[col].astype(pd.Int64Dtype())

    assert not df.duplicated(subset=["id", "report_date"]).any(), (
        "Duplicate (id, report_date) pairs found in core_eia191__monthly_gas_storage"
    )

    return df
