"""Validate post-ETL state demand analysis output."""
import logging

import pytest

from pudl import validate as pv

logger = logging.getLogger(__name__)


@pytest.mark.parametrize(
    "df_name,expected_rows",
    [("predicted_state_hourly_demand", 6_706_318)],
)
def test_minmax_rows(pudl_out_orig, live_dbs, expected_rows, df_name):
    """Verify that output DataFrames don't have too many or too few rows.

    Args:
        pudl_out_orig: A PudlTabl output object.
        live_dbs: Boolean (wether we're using a live or testing DB).
        expected_rows (int): Expected number of rows that the dataframe should
            contain when all data is loaded and is output without aggregation.
        df_name (str): Shorthand name identifying the dataframe, corresponding
            to the name of the function used to pull it from the PudlTabl
            output object.
    """
    if not live_dbs:
        pytest.skip("Data validation only works with a live PUDL DB.")
    _ = (
        pudl_out_orig.__getattribute__(df_name)()
        .pipe(
            pv.check_min_rows, expected_rows=expected_rows, margin=0.0, df_name=df_name
        )
        .pipe(
            pv.check_max_rows, expected_rows=expected_rows, margin=0.0, df_name=df_name
        )
    )
