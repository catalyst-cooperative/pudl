"""Transform EIA Form 191 monthly underground natural gas storage data."""

import pandas as pd
from dagster import asset

import pudl.helpers
from pudl.logging_helpers import get_logger

logger = get_logger(__name__)


@asset(io_manager_key="pudl_io_manager")
def core_eia191__monthly_gas_storage(
    raw_eia191__data: pd.DataFrame,
) -> pd.DataFrame:
    """Transform raw EIA-191 monthly underground natural gas storage data.

    One row per storage reservoir per month, identified by the EIA-assigned
    reservoir id and report_date.

    Transformations include:

    * Combine ``year`` and ``month`` into a single ``report_date`` column.
    * Normalize free-text string columns with
      :func:`pudl.helpers.simplify_strings` (lower-case, strip whitespace,
      remove non-standard characters).
    * Pass ``base_gas_mcf``, ``working_gas_capacity_mcf``, and
      ``total_field_capacity_mcf`` through as reported. These are operator
      self-reports under a loose EIA definition and are not additively
      consistent: ``total`` does not reliably equal ``working + base`` (~23%
      of rows differ). Do not assume additivity.

    Args:
        raw_eia191__data: Concatenated raw CSV data from the PUDL EIA-191
            Zenodo archive (RP8 monthly dataset, 2014–present).

    Returns:
        Cleaned monthly gas storage DataFrame with a combined report_date column.
    """
    df = raw_eia191__data.copy()

    df["reservoir_id_eia"] = df["reservoir_id_eia"].replace(999, pd.NA)

    df = pudl.helpers.simplify_strings(
        df,
        columns=[
            "company_name",
            "field_name",
            "reservoir_name",
            "field_type",
            "county",
            "operational_status",
            "region",
        ],
    )

    df = pudl.helpers.convert_to_date(
        df, date_col="report_date", year_col="year", month_col="month"
    )

    assert not df.duplicated(subset=["storage_field_id_eia191", "report_date"]).any(), (
        "Duplicate (storage_field_id_eia191, report_date) pairs found in core_eia191__monthly_gas_storage"
    )

    return df
