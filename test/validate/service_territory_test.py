"""Validate post-ETL FERC 714 outputs and associated service territory analyses."""

import logging

import pandas as pd
import pyarrow.parquet as pq
import pytest

from pudl.output.pudltabl import PudlTabl
from pudl.workspace.setup import PudlPaths

logger = logging.getLogger(__name__)


@pytest.mark.parametrize(
    "df_name,expected_rows",
    [
        ("summarized_demand_ferc714", 3_195),
        ("fipsified_respondents_ferc714", 136_011),
        ("compiled_geometry_balancing_authority_eia861", 113_142),
        ("compiled_geometry_utility_eia861", 256_949),
    ],
)
def test_minmax_rows(
    pudl_out_orig: PudlTabl,
    live_dbs: bool,
    df_name: str,
    expected_rows: int,
):
    """Verify that output DataFrames don't have too many or too few rows.

    Args:
        pudl_out_orig: A PudlTabl output object.
        live_dbs: Whether we're using a live or testing DB.
        df_name: Shorthand name identifying the dataframe, corresponding to the name of
            the function used to pull it from the PudlTabl output object.
        expected_rows: Expected number of rows that the dataframe should contain when
            all data is loaded and is output without aggregation.
    """
    if not live_dbs:
        pytest.skip("Data validation only works with a live PUDL DB.")
    assert pudl_out_orig.__getattribute__(df_name)().shape[0] == expected_rows


@pytest.mark.parametrize(
    "resource_id,expected_rows",
    [("out_ferc714__hourly_planning_area_demand", 15_608_154)],
)
def test_minmax_rows_and_year_in_ferc714_hourly_planning_area_demand(
    live_dbs: bool,
    resource_id: str,
    expected_rows: int,
):
    """Test if the majority of the years in the two date columns line up & min/max rows.

    We are parameterizing this test even though it only has one input because the
    test_minmax_rows is a common test across many tables and we wanted to preserve the
    format.
    """
    if not live_dbs:
        pytest.skip("Data validation only works with a live PUDL DB.")
    parquet_path = PudlPaths().parquet_path(resource_id)
    meta = pq.read_metadata(parquet_path)
    assert meta.num_rows == expected_rows
    hpad_ferc714 = pd.read_parquet(parquet_path, dtype_backend="pyarrow")

    logger.info("Checking the consistency of the year in the multiple date columns.")
    mismatched_report_years = hpad_ferc714[
        (hpad_ferc714.datetime_utc.dt.year != hpad_ferc714.report_date.dt.year)
    ]
    if (off_ratio := len(mismatched_report_years) / len(hpad_ferc714)) > 0.001:
        raise AssertionError(
            f"Found more ({off_ratio:.2%}) than expected (>.1%) FERC-714 records "
            "where the report year from the datetime_utc differs from the "
            "report_date column."
        )
