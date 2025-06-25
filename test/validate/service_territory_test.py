"""Validate post-ETL FERC 714 outputs and associated service territory analyses."""

import logging

import pandas as pd
import pytest

from pudl.workspace.setup import PudlPaths

logger = logging.getLogger(__name__)


def test_year_in_ferc714_hourly_planning_area_demand(live_dbs: bool):
    """Test if the majority of the years in the two date columns are identical."""
    if not live_dbs:
        pytest.skip("Data validation only works with a live PUDL DB.")

    parquet_path = PudlPaths().parquet_path("out_ferc714__hourly_planning_area_demand")
    hpad_ferc714 = pd.read_parquet(parquet_path, dtype_backend="pyarrow")

    logger.info("Checking the consistency of the year in the multiple date columns.")
    mismatched_report_years = hpad_ferc714[
        (hpad_ferc714.datetime_utc.dt.year != hpad_ferc714.report_date.dt.year)
    ]
    if (off_ratio := len(mismatched_report_years) / len(hpad_ferc714)) > 0.001:
        raise AssertionError(
            f"Found more ({off_ratio:.2%}) than expected (>0.1%) FERC-714 records "
            "where the report year from the datetime_utc differs from the "
            "report_date column."
        )
